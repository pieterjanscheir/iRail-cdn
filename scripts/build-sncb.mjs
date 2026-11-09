// Builds one validated NMBS/SNCB LC2-alpha release into a staging directory:
// archive -> extracted copy -> gtfs2lc (latest feat-arrow) -> iRail static enrichment -> integrity catalog -> validation.
// The result is a staging tree (manifest.jsonld + releases/<id>/) for scripts/assemble-pages.mjs.
import { createHash } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { mkdir, readFile, rm, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { parseArgs } from 'node:util';
import { pathToFileURL } from 'node:url';
import { parse } from 'csv-parse/sync';
import { convertGtfs, extractArchive, verifyConverter } from './convert-gtfs.mjs';
import { buildEnrichment } from './lib/enrich.mjs';
import { finalizeRelease, releaseSize } from './lib/release.mjs';
import { readStations, verifyStations } from './lib/stations.mjs';
import { reduceToStations } from './lib/stops.mjs';
import { addDays, brusselsDate, formatBytes, normalizePrefix, readJson, sha256File, walk } from './lib/util.mjs';
import { validateRelease } from './validate-release.mjs';

const root = path.resolve(import.meta.dirname, '..');
/**
 * Fingerprint of the code, contracts and configuration that shape the output: a pipeline change must not reuse a
 * release id. config/ is hashed whole (rights, license, attribution, converter profile and station pin all reach the
 * published files), and src/iri-strategy.ts mints the enrichment IRIs. The `realtime` block of config/sncb.json is
 * left out: realtime is served elsewhere, and the feeds a release does advertise are part of its id on their own.
 */
export async function pipelineFingerprint(base = root) {
  const files = [
    'scripts/build-sncb.mjs', 'scripts/convert-gtfs.mjs', 'specs/schemas.mjs', 'specs/lc-core-schemas.json', 'src/iri-strategy.ts',
    ...(await walk(path.join(base, 'scripts/lib'))).map((name) => `scripts/lib/${name}`),
    ...(await walk(path.join(base, 'config'))).map((name) => `config/${name}`),
  ];
  const hash = createHash('sha256');
  for (const file of files) {
    let contents = await readFile(path.join(base, file));
    if (file === 'config/sncb.json') {
      const { realtime, ...rest } = JSON.parse(contents);
      contents = JSON.stringify(rest);
    }
    hash.update(file).update(contents);
  }
  return hash.digest('hex');
}
const sha8 = (...parts) => createHash('sha256').update(JSON.stringify(parts)).digest('hex').slice(0, 8);

export async function downloadArchive(url, file, { fetcher = fetch, maxBytes = 1_000_000_000 } = {}) {
  const parsed = new URL(url);
  if (parsed.protocol !== 'https:' && parsed.hostname !== 'localhost' && parsed.hostname !== '127.0.0.1') throw new Error('The GTFS archive must be fetched over HTTPS');
  const response = await fetcher(url, { redirect: 'follow' });
  if (!response.ok || !response.body) throw new Error(`GTFS download failed: HTTP ${response.status}`);
  await mkdir(path.dirname(file), { recursive: true });
  await pipeline(Readable.fromWeb(response.body), createWriteStream(file));
  const { size } = await stat(file);
  if (size < 1024 || size > maxBytes) throw new Error(`GTFS archive has an implausible size: ${size} bytes`);
  const head = (await readFile(file)).subarray(0, 4).toString('latin1');
  if (!head.startsWith('PK')) throw new Error('GTFS download is not a zip archive');
  return file;
}

async function feedWindow(gtfsDir) {
  const text = await readFile(path.join(gtfsDir, 'feed_info.txt'), 'utf8').catch(() => '');
  const row = text ? parse(text, { columns: true, bom: true, skip_empty_lines: true })[0] ?? {} : {};
  return { version: row.feed_version || null, start: row.feed_start_date || null, end: row.feed_end_date || null, publisher: row.feed_publisher_name || null };
}

async function countRows(gtfsDir, name) {
  const text = await readFile(path.join(gtfsDir, name), 'utf8').catch(() => '');
  return text ? parse(text, { columns: true, bom: true, skip_empty_lines: true }).length : 0;
}

/**
 * @param {object} options
 * @param {string} [options.archive] local GTFS zip; otherwise `config.gtfs.url` is downloaded
 * @param {string} [options.stationsDir] checkout of github.com/iRail/stations at the commit in config/stations.json; without it
 *   the `stations` dataset is declared unavailable and the timetable is still built
 * @param {Array<object>|false} [options.realtime] GTFS Realtime feed descriptors for the manifest; defaults to config/sncb.json
 *   (`realtime.enabled`). The converter verifies every template against the trip IRIs.
 * @param {string} [options.gtfsDir] an already extracted directory (tests); copied by the caller if it must stay pristine
 */
export async function buildSncb(options) {
  const started = Date.now();
  const config = await readJson(path.join(root, 'config/sncb.json'));
  const pin = await readJson(path.join(root, 'config/converter.json'));
  const stationsPin = await readJson(path.join(root, 'config/stations.json'));
  const strategyPath = path.join(root, 'config/sources.json');
  const strategyConfig = await readJson(strategyPath);
  const prefix = normalizePrefix(options.prefix ?? '');
  const sourceId = config.sourceId;
  const workDir = path.resolve(options.workDir ?? 'build/work');
  const stagingDir = path.resolve(options.stagingDir ?? 'build/staging');
  const converterDir = path.resolve(options.converterDir ?? process.env.GTFS2LC_DIR ?? path.join(root, '..', 'gtfs2lc'));
  const converterInfo = await verifyConverter(converterDir, pin, { allowModified: options.allowModified });

  const realtime = options.realtime === false ? undefined : options.realtime ?? (config.realtime?.enabled ? config.realtime.feeds : undefined);
  let realtimePath;
  if (realtime) {
    await mkdir(workDir, { recursive: true });
    realtimePath = path.join(workDir, 'realtime.json');
    await writeFile(realtimePath, `${JSON.stringify(realtime, null, 2)}\n`);
  }
  let curated = null;
  if (options.stationsDir) {
    const stationsDir = path.resolve(options.stationsDir);
    const checkout = await verifyStations(stationsDir, stationsPin, { allowUnpinned: options.allowUnpinnedStations ?? options.allowUnpinned });
    curated = { ...await readStations(stationsDir, stationsPin), ...checkout };
  }

  let archive = { sha256: null, byteLength: null };
  let gtfsDir = options.gtfsDir ? path.resolve(options.gtfsDir) : path.join(workDir, 'gtfs');
  if (!options.gtfsDir) {
    // The converter normalizes the extracted files in place; the archive itself is never modified.
    let file = options.archive ? path.resolve(options.archive) : null;
    if (!file) file = await downloadArchive(options.url ?? config.gtfs.url, path.join(workDir, 'gtfs.zip'));
    archive = await extractArchive(file, gtfsDir);
    archive.sha256 = await sha256File(file);
  }
  // One stop per station in the core; platforms become the `platforms` extension (see scripts/lib/stops.mjs).
  const reduced = await reduceToStations(gtfsDir);
  const feed = await feedWindow(gtfsDir);
  const today = options.today ?? brusselsDate();
  const horizon = config.horizon;
  let startDate = options.startDate ?? addDays(today, -(options.daysBack ?? horizon.daysBack));
  let endDate = options.endDate ?? addDays(today, options.daysForward ?? horizon.daysForward);
  // Publish the coverage the source really has instead of a window it cannot supply.
  if (feed.start && startDate < feed.start) startDate = feed.start;
  if (feed.end && endDate > feed.end) endDate = feed.end;
  if (startDate > endDate) throw new Error(`The feed (${feed.start}..${feed.end}) does not cover the requested horizon ${startDate}..${endDate}`);
  const regions = options.regions ?? config.regions;
  const inputs = sha8(archive.sha256 ?? feed.version, await pipelineFingerprint(), converterInfo.commit, curated?.sha256 ?? null, realtime ?? null, strategyConfig, startDate, endDate, regions, prefix);
  const releaseId = options.releaseId ?? `${sourceId.replace(/^[a-z]+\./u, '')}-${today}-${inputs}`;

  if (options.previousDir) {
    const existing = path.join(path.resolve(options.previousDir), 'releases', releaseId);
    if (await stat(existing).then(() => true, () => false)) return { status: 'unchanged', releaseId, reason: 'identical inputs are already published' };
  }

  console.error(`Converting ${startDate}..${endDate} as ${releaseId} (feed ${feed.version ?? 'unversioned'})`);
  const { converter, releaseDir } = await convertGtfs({
    gtfsDir, stagingDir, converterDir, converterSource: pin, strategyPath, sourceId, releaseId, startDate, endDate, urlPrefix: prefix, regions,
    license: config.license ?? undefined, rights: config.rights ?? undefined, realtimePath, memoryMb: options.memoryMb, allowModified: options.allowModified,
  });
  const afterConversion = Date.now();
  const enrichment = await buildEnrichment({ gtfsDir, releaseDir, releaseId, sourceId, strategyConfig, stations: curated?.stations, platforms: reduced.platforms });
  const frequencies = await countRows(gtfsDir, 'frequencies.txt');
  const pathways = await countRows(gtfsDir, 'pathways.txt');
  const finalized = await finalizeRelease({
    stagingDir, releaseId, prefix, sourceId, strategy: enrichment.strategy, strategySha256: enrichment.strategySha256,
    sources: [{
      id: 'gtfs-static', description: config.gtfs.attribution, rights: config.rights ?? null, license: config.license ?? null,
      archive: { sha256: archive.sha256, byteLength: archive.byteLength },
      feed: { version: feed.version, publisher: feed.publisher, startDate: feed.start, endDate: feed.end },
      converter: { name: 'gtfs2lc', repository: pin.repository, ref: pin.ref, commit: converter.commit, version: converter.version, profile: pin.profile },
    }, ...(realtime ? [{
      id: 'gtfs-rt', description: 'GTFS Realtime TripUpdates republished by iRail from the open Belgian Mobility feed (NMBS/SNCB, CC BY 4.0); latest snapshot only',
      license: 'https://creativecommons.org/licenses/by/4.0/', feeds: realtime.map((feed) => feed['@id']),
    }] : []), ...(curated ? [{
      id: 'irail-stations', description: stationsPin.description, license: stationsPin.license,
      repository: stationsPin.repository, branch: stationsPin.branch, commit: curated.commit, pinnedCommit: stationsPin.commit,
      file: { path: stationsPin.file, sha256: curated.sha256, byteLength: curated.byteLength },
    }] : [])],
    capabilities: {
      stations: curated ? 'available' : 'unavailable',
      headsigns: enrichment.outputs.headsigns ? 'available' : 'unavailable',
      platforms: enrichment.outputs.platforms ? 'available' : 'unavailable',
      connections: 'available', stops: 'available', trips: 'available', routes: 'available', transfers: 'available',
      stopEnrichment: 'available', tripEnrichment: 'available', routeEnrichment: 'available',
      connectionEnrichment: 'unavailable', transferEnrichment: 'unavailable', shapes: 'unavailable',
      realtime: realtime ? 'available' : 'unconfigured', compositions: 'unavailable', occupancy: 'unavailable', alerts: 'unavailable', stopStatus: 'unavailable', delaysCancellations: 'unavailable',
    },
    diagnostics: {
      unsupportedGtfs: { frequencies, pathways },
      stopReduction: { stations: reduced.stations, droppedParents: reduced.dropped.parents, platforms: reduced.dropped.platforms, remappedTransfers: reduced.remappedTransfers },
      ...(enrichment.headsignDiagnostics ? { headsigns: enrichment.headsignDiagnostics } : {}),
      ...(enrichment.stationDiagnostics ? { stations: enrichment.stationDiagnostics } : {}),
      notes: [
        'The source feed has no shapes.txt content; shapes stay unavailable rather than being inferred from station pairs.',
        'The core stops table has one stop per station (its IRI is the iRail station IRI); GTFS parent stations and platform stops are not core rows. Platforms are published in platforms.arrow as <station IRI>#<platform code>.',
        'Connection IRIs use the stable https://irail.be/id/be.sncb/ namespace: several scheduled trips share a vehicle number on one day, so legacy vehicle/date connection IRIs are not unique.',
        'stop_time_overrides.txt (platform overrides) is not read by the converter and is not yet published.',
      ],
    },
  });
  const report = await validateRelease(stagingDir, releaseId, { prefix, deep: true });
  const bytes = await releaseSize(releaseDir);
  const result = {
    status: 'built', releaseId, prefix, startDate, endDate, regions, generatedAt: finalized.manifest.generatedAt, bytes,
    rows: report.rows, pages: report.pages, maxPageRows: report.maxPageRows,
    archive, feed, converter: { commit: converter.commit, ref: converter.ref },
    seconds: { conversion: (afterConversion - started) / 1000, total: (Date.now() - started) / 1000 },
  };
  await writeFile(path.join(stagingDir, 'build-result.json'), `${JSON.stringify(result, null, 2)}\n`);
  console.error(`Built ${releaseId}: ${formatBytes(bytes)}, ${report.rows.connections} connections in ${report.pages} pages (${result.seconds.total}s)`);
  return result;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const { values } = parseArgs({ options: {
    archive: { type: 'string' }, url: { type: 'string' }, 'gtfs-dir': { type: 'string' }, converter: { type: 'string' },
    work: { type: 'string' }, staging: { type: 'string' }, previous: { type: 'string' }, prefix: { type: 'string' },
    'start-date': { type: 'string' }, 'end-date': { type: 'string' }, 'days-back': { type: 'string' }, 'days-forward': { type: 'string' },
    stations: { type: 'string' }, realtime: { type: 'boolean' }, 'release-id': { type: 'string' }, regions: { type: 'string' }, 'memory-mb': { type: 'string' }, 'allow-unpinned': { type: 'boolean' }, 'allow-modified': { type: 'boolean' },
  } });
  const number = (value) => (value === undefined ? undefined : Number(value));
  try {
    const result = await buildSncb({
      realtime: values.realtime ? (await readJson(path.join(root, 'config/sncb.json'))).realtime.feeds : undefined,
      archive: values.archive, url: values.url, gtfsDir: values['gtfs-dir'], stationsDir: values.stations ?? process.env.STATIONS_DIR, converterDir: values.converter, workDir: values.work, stagingDir: values.staging,
      previousDir: values.previous, prefix: values.prefix, startDate: values['start-date'], endDate: values['end-date'],
      daysBack: number(values['days-back']), daysForward: number(values['days-forward']), releaseId: values['release-id'],
      regions: number(values.regions), memoryMb: number(values['memory-mb']), allowUnpinned: values['allow-unpinned'], allowModified: values['allow-modified'],
    });
    console.log(JSON.stringify(result));
    if (result.status === 'unchanged') await rm(path.resolve(values.staging ?? 'build/staging'), { recursive: true, force: true });
  } catch (error) {
    console.error(error.stack ?? error);
    process.exit(1);
  }
}
