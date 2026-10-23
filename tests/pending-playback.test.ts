import 'reflect-metadata';
import {Readable} from 'node:stream';
import {EventEmitter} from 'node:events';
import {AudioPlayerStatus, createAudioResource, StreamType} from '@discordjs/voice';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';

const mocks = vi.hoisted(() => ({execa: vi.fn(), join: vi.fn()}));
vi.mock('@discordjs/voice', async importOriginal => ({...await importOriginal<typeof import('@discordjs/voice')>(), joinVoiceChannel: mocks.join}));
vi.mock('execa', () => ({execa: mocks.execa}));
vi.mock('../src/services/file-cache.js', () => ({default: class {}}));
vi.mock('../src/utils/get-guild-settings.js', () => ({getGuildSettings: vi.fn(async () => ({secondsToWaitAfterQueueEmpties: 30, autoAnnounceNextSong: false}))}));
vi.mock('../src/utils/build-embed.js', () => ({buildPlayingMessageEmbed: vi.fn()}));
import Player, {MediaSource, STATUS} from '../src/services/player.js';
import PauseCommand from '../src/commands/pause.js';
import {getYouTubeMediaSource, YtDlpMediaUnavailableError} from '../src/utils/yt-dlp.js';

const cleanup: Array<() => void> = [];
const flush = async () => {for (let i = 0; i < 40; i++) await Promise.resolve();};
const nextTick = () => new Promise<void>(resolve => process.nextTick(resolve));
const deferred = <T,>() => {let resolve!: (value: T) => void; const promise = new Promise<T>(done => {resolve = done;}); return {promise, resolve};};
const song = (isLive = false, url = 'abcdefghijk') => ({title: 'Track', artist: 'Artist', url, length: 1200, offset: 0, isLive, source: MediaSource.Youtube, playlist: null, thumbnailUrl: null, addedInChannelId: 'text', requestedBy: 'user'});
const makePlayer = (options: {live?: boolean; buffered?: boolean; resolver?: any} = {}) => {
  const player = new Player({} as never, 'review-guild', options.resolver);
  const input = new Readable({objectMode: true, read() {}});
  const connection = {state: {status: 'ready'}, destroy: vi.fn(), setSpeaking: vi.fn(), prepareAudioPacket: vi.fn(), dispatchAudio: vi.fn(), subscribe(audio: any) {return audio.subscribe(connection);}};
  player.voiceConnection = connection as never;
  const getStream = vi.fn(async () => input);
  Object.assign(player, {getStream, createAudioStream: (stream: Readable) => createAudioResource(stream, {inputType: StreamType.Opus, silencePaddingFrames: 0})});
  player.add(song(options.live));
  if (options.buffered !== false) input.push(Buffer.from([0xf8, 0xff, 0xfe]));
  cleanup.push(() => {player.disconnect(); input.destroy();});
  return {player, input, getStream, connection, state: player as any};
};
beforeEach(() => {
  vi.useFakeTimers();
  mocks.execa.mockReset();
  vi.spyOn(console, 'info').mockImplementation(() => {});
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.spyOn(console, 'error').mockImplementation(() => {});
});
afterEach(() => {for (const close of cleanup.splice(0)) close(); vi.clearAllTimers(); vi.useRealTimers(); vi.restoreAllMocks();});

describe('real pending playback cancellation and terminal events', () => {
  it.each(['song', 'queue', 'both'])('settles a terminated live source with %s loop selected', async loop => {
    const {player,input,state} = makePlayer({live:true});
    await player.play();
    player.loopCurrentSong = loop !== 'queue';
    player.loopCurrentQueue = loop !== 'song';
    state.audioPlayer._stepPrepare();
    input.push(null); input.read(0); await nextTick();
    state.audioPlayer.checkPlayable();
    await flush();
    expect(state.audioPlayer.state.status).toBe(AudioPlayerStatus.Idle);
    expect(player.status).toBe(STATUS.IDLE);
  });

  it('the actual pause command can cancel a pending Playing confirmation', async () => {
    const {player,input,state} = makePlayer({buffered:false});
    const pending = player.play(); void pending.catch(() => {}); await flush();
    expect(state.audioPlayer.state.status).toBe(AudioPlayerStatus.Buffering);
    const command = new PauseCommand({get: () => player} as never);
    const commandResult = command.execute({guild:{id:'review-guild'}, reply:vi.fn()} as never);
    void commandResult.catch(() => {});
    input.push(Buffer.from([0xf8, 0xff, 0xfe])); await pending;
    await expect(commandResult).resolves.toBeUndefined();
    expect(player.status).toBe(STATUS.PAUSED);
  });

  it('pause during Buffering prevents real packets after the pending wait resolves', async () => {
    const {player,input,state,connection} = makePlayer({buffered:false});
    const pending = player.play(); await flush();
    expect(state.audioPlayer.state.status).toBe(AudioPlayerStatus.Buffering);
    const audio = state.audioPlayer;
    player.pause();
    input.push(Buffer.from([0xf8, 0xff, 0xfe])); await pending;
    expect(player.status).toBe(STATUS.PAUSED);
    audio._stepPrepare(); audio._stepDispatch();
    expect(connection.prepareAudioPacket).not.toHaveBeenCalled();
    expect(audio.state.status).not.toBe(AudioPlayerStatus.Playing);
  });

  it('pause during a pending age fallback cancels it before replacement starts', async () => {
    const fallback = deferred<any>();
    const resolver = vi.fn(() => fallback.promise);
    const {player,getStream} = makePlayer({resolver});
    getStream.mockRejectedValueOnce(new YtDlpMediaUnavailableError('Sign in to confirm your age', 'age-restricted'));
    const pending = player.play(); await flush();
    expect(resolver).toHaveBeenCalledOnce();
    let pauseFailure: unknown;
    try {player.pause();} catch (error) {pauseFailure = error;}
    fallback.resolve(song(false, 'lmnopqrstuv')); await pending;
    expect(pauseFailure).toBeUndefined();
    expect(player.getCurrent()?.url).toBe('abcdefghijk');
    expect(player.status).toBe(STATUS.PAUSED);
  });

  it('an error immediately after the Playing event cannot leave a dead player marked Playing', async () => {
    const {player,input,state} = makePlayer({buffered:false});
    const pending = player.play(); await flush();
    state.audioPlayer.once(AudioPlayerStatus.Playing, () => {
      state.audioResource.playStream.emit('error', new Error('decoder failed immediately after first packet'));
    });
    const audio = state.audioPlayer;
    const rejection = expect(pending).rejects.toThrow('stopped before startup was confirmed');
    void rejection.catch(() => {});
    input.push(Buffer.from([0xf8, 0xff, 0xfe])); await rejection; await flush();
    expect(audio.state.status).toBe(AudioPlayerStatus.Idle);
    expect(player.status).toBe(STATUS.PAUSED);
    expect(state.audioPlayer).toBeNull();
  });

  it('a cancelled pending seek can resume on the first retry rather than reusing its stopped player', async () => {
    const {player,state} = makePlayer();
    await player.play();
    delete state.getStream; // use the production cache/extraction/ownership path for the seek and resume
    const delayed = deferred<{stdout:string}>();
    const response = {stdout:JSON.stringify({url:'https://media.invalid/audio',is_live:false})};
    mocks.execa.mockReturnValueOnce(delayed.promise).mockResolvedValue(response);
    Object.assign(player, {
      fileCache:{getEntryFor:vi.fn(async () => null)},
      createReadStream:vi.fn(async () => {
        const stream = new Readable({objectMode:true,read() {}});
        stream.push(Buffer.from([0xf8, 0xff, 0xfe])); cleanup.push(() => stream.destroy()); return stream;
      }),
    });
    const pendingSeek = player.seek(20); void pendingSeek.catch(() => {}); await flush();
    expect(state.audioPlayer.state.status).toBe(AudioPlayerStatus.Idle);
    expect(mocks.execa).toHaveBeenCalledOnce();
    player.pause();
    delayed.resolve(response); await pendingSeek.catch(() => {});
    let resumeFailure:unknown;
    const resumed = player.play().catch(error => {resumeFailure = error;}); await flush();
    await vi.advanceTimersByTimeAsync(15_000); await resumed;
    expect(resumeFailure).toBeUndefined();
  });

  it('keeps the real Playing timeout at 15 seconds and permits immediate retry', async () => {
    const {player, input, state, getStream} = makePlayer({buffered: false});
    let settled = false;
    const pending = player.play();
    void pending.finally(() => {settled = true;}).catch(() => {});
    const rejection = expect(pending).rejects.toMatchObject({name: 'AbortError'});
    await flush();
    await vi.advanceTimersByTimeAsync(14_999);
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await rejection;
    expect(player.status).toBe(STATUS.PAUSED);
    expect(state.audioPlayer).toBeNull();
    expect(input.destroyed).toBe(true);
    const retryInput = new Readable({objectMode: true, read() {}});
    cleanup.push(() => retryInput.destroy());
    retryInput.push(Buffer.from([0xf8, 0xff, 0xfe]));
    getStream.mockResolvedValueOnce(retryInput);
    await player.play();
    expect(player.status).toBe(STATUS.PLAYING);
  });

  it('logs only safe media identifiers during age-restricted fallback', async () => {
    const resolver = vi.fn(async () => song(false, 'https://youtube.invalid/watch?sig=fallback-secret'));
    const {player, getStream} = makePlayer({resolver});
    getStream.mockRejectedValueOnce(new YtDlpMediaUnavailableError('Sign in to confirm your age', 'age-restricted'));
    await player.play();
    expect(player.status).toBe(STATUS.PLAYING);
    expect(JSON.stringify(vi.mocked(console.warn).mock.calls)).not.toContain('fallback-secret');
  });

  it('unavailable extraction errors do not bypass playback redaction', async () => {
    mocks.execa.mockRejectedValue({stderr: 'ERROR: Video unavailable https://media.invalid/audio?sig=review-secret\nAuthorization: Bearer review-credential'});
    const {player,getStream} = makePlayer();
    getStream.mockImplementationOnce(async () => {await getYouTubeMediaSource('abcdefghijk'); throw new Error('unreachable');});
    await player.play();
    const output = JSON.stringify(vi.mocked(console.warn).mock.calls);
    expect(output).not.toContain('review-secret');
    expect(output).not.toContain('review-credential');
  });
});


describe('voice connection error privacy', () => {
  it('logs bounded sanitized text without external Error properties', async () => {
    const connection = Object.assign(new EventEmitter(), {state: {status: 'ready'}, destroy: vi.fn(), receiver: {speaking: new EventEmitter()}});
    mocks.join.mockReturnValue(connection);
    const player = new Player({} as never, 'review-guild');
    await player.connect({id: 'channel', guild: {id: 'review-guild', voiceAdapterCreator: {}}} as never);
    connection.emit('error', Object.assign(new Error('WebSocket failed https://voice.invalid/?token=voice-url-secret Authorization: Bearer voice-header-secret'), {token: 'voice-property-secret'}));
    const output = JSON.stringify(vi.mocked(console.error).mock.calls);
    for (const secret of ['voice-url-secret', 'voice-header-secret', 'voice-property-secret']) expect(output).not.toContain(secret);
    expect(vi.mocked(console.error).mock.calls[0][1]).toBeTypeOf('string');
    connection.emit('error', new Error('x'.repeat(900)));
    expect(String(vi.mocked(console.error).mock.calls.at(-1)![1]).length).toBeLessThanOrEqual(500);
    player.disconnect();
  });
});
