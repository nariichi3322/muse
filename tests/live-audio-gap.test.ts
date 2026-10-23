import 'reflect-metadata';
import {Readable} from 'node:stream';
import {AudioPlayerStatus, createAudioResource, StreamType} from '@discordjs/voice';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';
vi.mock('../src/services/file-cache.js', () => ({default: class {}}));
vi.mock('../src/utils/get-guild-settings.js', () => ({getGuildSettings: vi.fn().mockResolvedValue({secondsToWaitAfterQueueEmpties: 0})}));
vi.mock('../src/utils/build-embed.js', () => ({buildPlayingMessageEmbed: vi.fn()}));
import {getGuildSettings} from '../src/utils/get-guild-settings.js';
import Player, {MediaSource, STATUS} from '../src/services/player.js';

const cleanup: Array<() => void> = [];
beforeEach(() => {vi.useFakeTimers(); vi.mocked(getGuildSettings).mockResolvedValue({secondsToWaitAfterQueueEmpties: 0} as never);});
afterEach(() => {for (const close of cleanup.splice(0)) close(); vi.clearAllTimers(); vi.useRealTimers(); vi.restoreAllMocks();});

const start = async (live: boolean) => {
  const source = new Readable({objectMode: true, read() {}});
  const resource = createAudioResource(source, {inputType: StreamType.Opus, silencePaddingFrames: 0});
  const player = new Player({} as never, 'gap-guild');
  const connection = {state: {status: 'ready'}, destroy: vi.fn(), setSpeaking: vi.fn(), prepareAudioPacket: vi.fn(), dispatchAudio: vi.fn(), subscribe(audio: any) {audio.subscribe(connection);}};
  player.voiceConnection = connection as never;
  Object.assign(player, {ensureVoiceConnectionReady: vi.fn().mockResolvedValue(connection), getStream: vi.fn().mockResolvedValue(source), createAudioStream: () => resource});
  player.add({title: 'Gap source', artist: 'Artist', url: 'abcdefghijk', length: 1200, offset: 0, playlist: null, isLive: live, thumbnailUrl: null, source: MediaSource.Youtube, addedInChannelId: 'text', requestedBy: 'user'});
  source.push(Buffer.from([0xf8, 0xff, 0xfe]));
  await player.play();
  await new Promise<void>(resolve => process.nextTick(resolve));
  const audio = (player as any).audioPlayer;
  expect(audio.state.status).toBe(AudioPlayerStatus.Playing);
  audio._stepPrepare();
  const step = (count: number) => {for (let i = 0; i < count; i++) {if (audio.checkPlayable()) {audio._stepPrepare(); audio._stepDispatch();}}};
  const packet = () => {source.push(Buffer.from([0xf8, 0xff, 0xfe])); step(1);};
  cleanup.push(() => {player.disconnect(); source.destroy();});
  return {source, audio, step, packet, player};
};

describe('real voice player packet gaps (one prepare/dispatch step is 20ms)', () => {
  it.each([100,400])('keeps live playback through %i missed packets and resumes', async count => {
    const {source, audio, step, packet} = await start(true);
    step(count);
    expect(source.readableEnded).toBe(false);
    expect(source.destroyed).toBe(false);
    expect(audio.state.status).toBe(AudioPlayerStatus.Playing);
    packet();
    expect(audio.state.missedFrames).toBe(0);
    expect(audio.state.status).toBe(AudioPlayerStatus.Playing);
  });
  it('resets missed frames between two 400-packet gaps', async () => {
    const {audio, step, packet} = await start(true);
    step(400); packet(); step(400); packet();
    expect(audio.state.status).toBe(AudioPlayerStatus.Playing);
    expect(audio.state.missedFrames).toBe(0);
  });
  it.each([false,true])('ends after the configured finite gap limit (live=%s)', async live => {
    const {audio, step} = await start(live);
    step(live ? 499 : 49);
    expect(audio.state.status).toBe(AudioPlayerStatus.Playing);
    step(10);
    expect(audio.state.status).toBe(AudioPlayerStatus.Idle);
  });
});


describe('real incident: open source, Idle evidence, then queue timeout', () => {
  it.each(['eof','excess-gap'])('traces %s without an error and disconnects only at 30 seconds', async cause => {
    vi.mocked(getGuildSettings).mockResolvedValue({secondsToWaitAfterQueueEmpties: 30} as never);
    const log = vi.spyOn(console, 'info').mockImplementation(() => {});
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {});
    const {source,audio,step,player} = await start(true);
    step(100); expect(audio.state.status).toBe(AudioPlayerStatus.Playing); expect(source.destroyed).toBe(false);
    expect(log).not.toHaveBeenCalledWith('Playback diagnostic', expect.objectContaining({event: 'audio-idle'}));
    if (cause === 'eof') {
      source.push(null); source.read(0); await new Promise<void>(resolve => process.nextTick(resolve));
      audio.checkPlayable(); step(2);
    } else {step(501);}
    for (let i = 0; i < 30; i++) await Promise.resolve();
    expect(audio.state.status).toBe(AudioPlayerStatus.Idle); expect(player.status).toBe(STATUS.IDLE);
    expect(log).toHaveBeenCalledWith('Playback diagnostic', expect.objectContaining({event: 'audio-idle', isLive: true, reason: 'unknown', mediaId: 'abcdefghijk'}));
    await vi.advanceTimersByTimeAsync(29_999); expect(player.voiceConnection).not.toBeNull();
    await vi.advanceTimersByTimeAsync(1); expect(player.voiceConnection).toBeNull();
    expect(log).toHaveBeenCalledWith('Playback diagnostic', expect.objectContaining({event: 'disconnect', reason: 'queue-empty-timeout'}));
    expect(errors).not.toHaveBeenCalled();
  });

  it('advances a normal VOD EOF only once and respects zero empty-queue timeout', async () => {
    const {source,audio,step,player} = await start(false);
    const advance = vi.spyOn(player, 'manualForward');
    source.push(null); source.read(0); await new Promise<void>(resolve => process.nextTick(resolve));
    audio.checkPlayable(); step(10);
    for (let i = 0; i < 30; i++) await Promise.resolve();
    expect(advance).toHaveBeenCalledOnce(); expect(player.status).toBe(STATUS.IDLE);
    await vi.advanceTimersByTimeAsync(60_000); expect(player.voiceConnection).not.toBeNull();
  });
});
