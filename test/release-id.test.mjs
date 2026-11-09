import assert from 'node:assert/strict';
import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { pipelineFingerprint } from '../scripts/build-sncb.mjs';

const root = path.resolve(import.meta.dirname, '..');
const work = path.join(root, 'build', 'test-release-id');

/** A copy of everything the fingerprint reads, so a test can change one input at a time. */
async function copy(name) {
  const directory = path.join(work, name);
  await rm(directory, { recursive: true, force: true });
  await mkdir(directory, { recursive: true });
  for (const part of ['scripts', 'specs', 'src', 'config', 'README.md']) await cp(path.join(root, part), path.join(directory, part), { recursive: true });
  return directory;
}

async function edit(directory, file, change) {
  const target = path.join(directory, file);
  await writeFile(target, change(await readFile(target, 'utf8')));
}

const editJson = (directory, file, change) => edit(directory, file, (text) => `${JSON.stringify(change(JSON.parse(text)), null, 2)}\n`);

test('every input that reaches the published files changes the release fingerprint', async () => {
  const baseline = await pipelineFingerprint(await copy('baseline'));
  const changes = [
    ['rights', (directory) => editJson(directory, 'config/sncb.json', (config) => ({ ...config, rights: `${config.rights} Changed.` }))],
    ['license', (directory) => editJson(directory, 'config/sncb.json', (config) => ({ ...config, license: 'https://creativecommons.org/licenses/by/4.0/' }))],
    ['attribution', (directory) => editJson(directory, 'config/sncb.json', (config) => ({ ...config, gtfs: { ...config.gtfs, attribution: 'Changed' } }))],
    ['converter profile', (directory) => editJson(directory, 'config/converter.json', (pin) => ({ ...pin, profile: 'irail-cdn-v2' }))],
    ['stations description', (directory) => editJson(directory, 'config/stations.json', (pin) => ({ ...pin, description: 'Changed' }))],
    ['IRI strategy code', (directory) => edit(directory, 'src/iri-strategy.ts', (text) => `${text}\n// changed\n`)],
  ];
  for (const [name, change] of changes) {
    const directory = await copy(name.replaceAll(' ', '-'));
    await change(directory);
    assert.notEqual(await pipelineFingerprint(directory), baseline, `a change to the ${name} must produce a new release id`);
  }
});

test('files that do not shape the release leave the fingerprint unchanged', async () => {
  const baseline = await pipelineFingerprint(await copy('baseline'));
  const directory = await copy('readme');
  await edit(directory, 'README.md', (text) => `${text}\nChanged.\n`);
  assert.equal(await pipelineFingerprint(directory), baseline);
});

test('the realtime block of config/sncb.json leaves the fingerprint unchanged', async () => {
  const baseline = await pipelineFingerprint(await copy('baseline'));
  const directory = await copy('realtime');
  await editJson(directory, 'config/sncb.json', (config) => ({ ...config, realtime: { ...config.realtime, enabled: !config.realtime.enabled, note: 'Changed' } }));
  assert.equal(await pipelineFingerprint(directory), baseline, 'realtime metadata is served elsewhere and must not start a new release');
});
