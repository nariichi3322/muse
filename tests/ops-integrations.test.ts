import 'reflect-metadata';
import {promises as fs} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {PassThrough, Readable} from 'node:stream';
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest';

const dependencyMocks = vi.hoisted(() => ({
  createAudioPlayer: vi.fn(),
  createAudioResource: vi.fn(),
  debug: vi.fn(),
  entersState: vi.fn(),
  execa: vi.fn(),
  ffmpeg: vi.fn(),
  getGuildSettings: vi.fn(),
  joinVoiceChannel: vi.fn(),
  sleep: vi.fn(),
  spotifyClientCredentialsGrant: vi.fn(),
  spotifyConstructorOptions: [] as unknown[],
  spotifySetAccessToken: vi.fn(),
}));

vi.mock('@discordjs/voice', () => ({
  AudioPlayerStatus: {Idle: 'idle', Playing: 'playing'},
  VoiceConnectionDisconnectReason: {WebSocketClose: 'websocket-close'},
  VoiceConnectionStatus: {
    Connecting: 'connecting',
    Disconnected: 'disconnected',
    Destroyed: 'destroyed',
    Ready: 'ready',
    Signalling: 'signalling',
  },
  NetworkingStatusCode: {0: 'OpeningWs', 2: 'UdpHandshaking'},
  StreamType: {WebmOpus: 'webm-opus'},
  createAudioPlayer: dependencyMocks.createAudioPlayer,
  createAudioResource: dependencyMocks.createAudioResource,
  entersState: dependencyMocks.entersState,
  joinVoiceChannel: dependencyMocks.joinVoiceChannel,
}));

vi.mock('timers/promises', () => ({
  setTimeout: dependencyMocks.sleep,
}));

vi.mock('execa', () => ({
  execa: dependencyMocks.execa,
}));

vi.mock('fluent-ffmpeg', () => ({
  default: dependencyMocks.ffmpeg,
}));

vi.mock('prism-media', async () => {
  const {Transform} = await import('node:stream');

  return {
    default: {
      opus: {
        WebmDemuxer: class extends Transform {
          _transform(chunk: Buffer, _encoding: BufferEncoding, done: (error?: Error | null) => void) {
            if (chunk.includes(1)) {
              this.push(Buffer.from([0xf8, 0xff, 0xfe]));
            }

            done();
          }
        },
      },
    },
  };
});

vi.mock('spotify-web-api-node', () => ({
  default: class {
    clientCredentialsGrant = dependencyMocks.spotifyClientCredentialsGrant;
    setAccessToken = dependencyMocks.spotifySetAccessToken;

    constructor(options: unknown) {
      dependencyMocks.spotifyConstructorOptions.push(options);
    }
  },
}));

vi.mock('../src/services/file-cache.js', () => ({
  default: class {},
}));

vi.mock('../src/utils/build-embed.js', () => ({
  buildPlayingMessageEmbed: vi.fn(() => ({title: 'playing'})),
}));

vi.mock('../src/utils/get-guild-settings.js', () => ({
  getGuildSettings: dependencyMocks.getGuildSettings,
}));

vi.mock('../src/utils/debug.js', () => ({
  default: dependencyMocks.debug,
}));

import type {VoiceConnection} from '@discordjs/voice';
import type {PlaybackAttemptContext, PlaybackAttemptTracker} from '../src/services/playback-attempt.js';
import Player, {
  FfmpegMediaUnavailableError,
  FfmpegStartupError,
  MediaSource,
  QueuedSong,
} from '../src/services/player.js';
import ThirdParty from '../src/services/third-party.js';
import prepareYtDlp from '../src/utils/prepare-yt-dlp.js';
import {getExecutable, getSoundCloudMetadata, getSoundCloudMediaSource, getYouTubeMediaSource, getYtDlpVersion, updateYtDlp, YtDlpMediaUnavailableError} from '../src/utils/yt-dlp.js';

const GUILD_ID = 'guild-id';
const ORIGINAL_ENV = {
  DISCORD_TOKEN: process.env.DISCORD_TOKEN,
  MUSE_BUNDLED_YT_DLP_PATH: process.env.MUSE_BUNDLED_YT_DLP_PATH,
  PATH: process.env.PATH,
  SPOTIFY_CLIENT_ID: process.env.SPOTIFY_CLIENT_ID,
  SPOTIFY_CLIENT_SECRET: process.env.SPOTIFY_CLIENT_SECRET,
  YOUTUBE_API_KEY: process.env.YOUTUBE_API_KEY,
  YT_DLP_COOKIES_PATH: process.env.YT_DLP_COOKIES_PATH,
  YT_DLP_PATH: process.env.YT_DLP_PATH,
};
const VALID_MEDIA_RESPONSE = JSON.stringify({
  requested_downloads: [{
    url: 'https://media.example/requested.webm',
    http_headers: {
      Authorization: 'Bearer media-token',
      'User-Agent': 'Muse ops test',
      Empty: '',
      Missing: null,
    },
  }],
  http_headers: {'X-Fallback': 'not-used'},
  is_live: true,
});

const makeDeferred = <T>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {promise, reject, resolve};
};

const flushAsyncWork = async () => {
  for (let index = 0; index < 12; index++) {
    // eslint-disable-next-line no-await-in-loop
    await Promise.resolve();
  }
};

const makeFfmpegCommand = ({deferProcessResult = false, emptyOutput = false, hangOutput = false, headerOnlyOutput = false, startupError, startupErrorAfterOutput}: {
  deferProcessResult?: boolean;
  emptyOutput?: boolean;
  hangOutput?: boolean;
  headerOnlyOutput?: boolean;
  startupError?: Error;
  startupErrorAfterOutput?: Error;
} = {}) => {
  const handlers = new Map<string, (...args: unknown[]) => void>();
  const command = {
    audioCodec: vi.fn(() => command),
    inputOptions: vi.fn(() => command),
    kill: vi.fn(),
    noVideo: vi.fn(() => command),
    on: vi.fn((event: string, handler: (...args: unknown[]) => void) => {
      handlers.set(event, handler);
      return command;
    }),
    outputFormat: vi.fn(() => command),
    pipe: vi.fn((destination: {end(): void; write(chunk: Buffer): void}) => {
      if (startupError) {
        handlers.get('error')?.(startupError);
        return destination;
      }

      if (hangOutput) {
        return destination;
      }

      if (!emptyOutput) {
        destination.write(Buffer.from([headerOnlyOutput ? 2 : 1]));
      }
      destination.end();
      if (deferProcessResult) {
        return destination;
      } else if (startupErrorAfterOutput) {
        handlers.get('error')?.(startupErrorAfterOutput);
      } else {
        handlers.get('end')?.();
      }
      return destination;
    }),
    triggerEnd: () => handlers.get('end')?.(),
    triggerError: (error: Error) => handlers.get('error')?.(error),
    triggerStart: () => handlers.get('start')?.(),
  };

  return command;
};

const makeExecutableLayout = async (includePython = true) => {
  const root = await fs.mkdtemp(path.join(tmpdir(), 'muse-ops-yt-dlp-'));
  const bin = path.join(root, 'bin');
  const executable = path.join(bin, 'yt-dlp');
  const python = path.join(bin, 'python');
  await fs.mkdir(bin);
  await fs.writeFile(executable, '#!/bin/sh\n', {mode: 0o755});
  if (includePython) {
    await fs.writeFile(python, '#!/bin/sh\n', {mode: 0o755});
  }

  return {executable, python, root};
};

const loadSpotifyBindingHarness = async (clientId: string, clientSecret: string) => {
  vi.resetModules();
  process.env.DISCORD_TOKEN = 'synthetic-discord-token';
  process.env.YOUTUBE_API_KEY = 'synthetic-youtube-key';
  process.env.SPOTIFY_CLIENT_ID = clientId;
  process.env.SPOTIFY_CLIENT_SECRET = clientSecret;
  vi.doMock('discord.js', async () => {
    const actual = await vi.importActual<typeof import('discord.js')>('discord.js');

    return {
      ...actual,
      Client: class {},
    };
  });

  const [{default: container}, {TYPES}, {default: Play}] = await Promise.all([
    import('../src/inversify.config.js'),
    import('../src/types.js'),
    import('../src/commands/play.js'),
  ]);

  return {container, Play, TYPES};
};

const makeVoiceConnection = (overrides: Record<string, unknown> = {}) => {
  const handlers = new Map<string, (...args: unknown[]) => void>();
  const connection = {
    destroy: vi.fn(),
    joinConfig: {channelId: 'voice-channel-id'},
    on: vi.fn((event: string, handler: (...args: unknown[]) => void) => {
      handlers.set(event, handler);
      return connection;
    }),
    receiver: {speaking: {on: vi.fn()}},
    rejoin: vi.fn(() => true),
    rejoinAttempts: 0,
    state: {status: 'ready'},
    subscribe: vi.fn(),
    ...overrides,
  };

  return {connection, handlers};
};

const getPrivatePlayer = (player: Player) => player as unknown as {
  onVoiceConnectionDisconnect(connection: object): Promise<void>;
};

const getOwnedStream = (player: Player, song: QueuedSong, options: {seek?: number; to?: number} = {}) => {
  if (player.getCurrent() !== song) {
    player.add(song);
    while (player.getCurrent() !== song) player.manualForward(1);
  }
  player.voiceConnection ??= makeVoiceConnection().connection as never;
  const internal = player as unknown as {
    playbackAttempts: PlaybackAttemptTracker<QueuedSong, VoiceConnection>;
    getStream(song: QueuedSong, options: {seek?: number; to?: number}, playback: PlaybackAttemptContext<QueuedSong, VoiceConnection>): Promise<Readable>;
  };
  const playback = internal.playbackAttempts.capture(internal.playbackAttempts.latest(), song, player.getCurrentQueueEntryId()!, player.voiceConnection!);
  return internal.getStream(song, options, playback);
};

beforeEach(() => {
  vi.clearAllMocks();
  dependencyMocks.spotifyConstructorOptions.length = 0;
  delete process.env.YT_DLP_COOKIES_PATH;
  delete process.env.YT_DLP_PATH;
  delete process.env.MUSE_BUNDLED_YT_DLP_PATH;
  dependencyMocks.execa.mockReset().mockResolvedValue({stdout: VALID_MEDIA_RESPONSE});
  dependencyMocks.ffmpeg.mockImplementation(makeFfmpegCommand);
  dependencyMocks.getGuildSettings.mockResolvedValue({
    defaultVolume: 100,
    turnDownVolumeWhenPeopleSpeak: false,
  });
  dependencyMocks.entersState.mockResolvedValue(undefined);
  dependencyMocks.sleep.mockResolvedValue(undefined);
});

afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.restoreAllMocks();

  for (const [name, value] of Object.entries(ORIGINAL_ENV)) {
    if (value === undefined) {
      delete process.env[name];
    } else {
      process.env[name] = value;
    }
  }
});

describe('OPS-10 yt-dlp selection and update lifecycle', () => {
  it('prefers YT_DLP_PATH and reports the trimmed installed version when auto-update is disabled', async () => {
    process.env.YT_DLP_PATH = '/configured/yt-dlp';
    process.env.MUSE_BUNDLED_YT_DLP_PATH = '/bundled/yt-dlp';
    process.env.PATH = '/path/that/would/otherwise/be/searched';
    dependencyMocks.execa.mockResolvedValue({stdout: ' 2026.07.12 \n'});
    const log = vi.spyOn(console, 'log').mockImplementation(() => undefined);

    expect(getExecutable()).toBe('/configured/yt-dlp');
    await expect(getYtDlpVersion()).resolves.toBe('2026.07.12');
    await prepareYtDlp({YT_DLP_AUTO_UPDATE: false} as never);

    expect(dependencyMocks.execa).toHaveBeenCalledWith('/configured/yt-dlp', ['--version'], {
      timeout: 15_000,
    });
    expect(log).toHaveBeenCalledWith('YT_DLP_VERSION=2026.07.12 (/configured/yt-dlp)');
  });

  it('falls back from a blank configured path to the bundled executable and then the PATH command', () => {
    process.env.YT_DLP_PATH = '   ';
    process.env.MUSE_BUNDLED_YT_DLP_PATH = ' /bundled/yt-dlp ';
    expect(getExecutable()).toBe('/bundled/yt-dlp');

    delete process.env.MUSE_BUNDLED_YT_DLP_PATH;
    expect(getExecutable()).toBe('yt-dlp');
  });

  it('prefers pip through the executable sibling virtualenv and re-probes the updated version', async () => {
    const layout = await makeExecutableLayout();
    const pythonExecutable = await fs.realpath(layout.python);
    process.env.YT_DLP_PATH = layout.executable;
    const versions = ['2026.01.01', '2026.07.12'];
    dependencyMocks.execa.mockImplementation(async (executable: string, args: string[]) => {
      if (args[0] === '--version') {
        return {stdout: versions.shift()!};
      }

      if (executable === pythonExecutable) {
        return {stdout: ''};
      }

      throw new Error(`Unexpected command: ${executable} ${args.join(' ')}`);
    });

    try {
      await expect(updateYtDlp()).resolves.toEqual({
        beforeVersion: '2026.01.01',
        afterVersion: '2026.07.12',
        updated: true,
        skipped: false,
        updateSucceeded: true,
        error: undefined,
      });
      expect(dependencyMocks.execa).toHaveBeenNthCalledWith(2, pythonExecutable, [
        '-m',
        'pip',
        'install',
        '--disable-pip-version-check',
        '--no-input',
        '--upgrade',
        'yt-dlp[default]',
      ], {
        env: {
          PIP_DISABLE_PIP_VERSION_CHECK: '1',
          PIP_NO_INPUT: '1',
        },
        timeout: 120_000,
      });
      expect(dependencyMocks.execa.mock.calls.some(([, args]) => (args as string[])[0] === '-U')).toBe(false);
      expect(dependencyMocks.execa).toHaveBeenLastCalledWith(layout.executable, ['--version'], {
        timeout: 15_000,
      });
    } finally {
      await fs.rm(layout.root, {recursive: true, force: true});
    }
  });

  it('falls back to self-update when sibling-venv pip fails and still re-probes', async () => {
    const layout = await makeExecutableLayout();
    const pythonExecutable = await fs.realpath(layout.python);
    process.env.YT_DLP_PATH = layout.executable;
    const versions = ['2026.01.01', '2026.07.12'];
    dependencyMocks.execa.mockImplementation(async (executable: string, args: string[]) => {
      if (args[0] === '--version') {
        return {stdout: versions.shift()!};
      }

      if (executable === pythonExecutable) {
        throw {stderr: 'pip update failed'};
      }

      if (executable === layout.executable && args[0] === '-U') {
        return {stdout: ''};
      }

      throw new Error(`Unexpected command: ${executable} ${args.join(' ')}`);
    });

    try {
      await expect(updateYtDlp()).resolves.toMatchObject({
        beforeVersion: '2026.01.01',
        afterVersion: '2026.07.12',
        updated: true,
        updateSucceeded: true,
      });
      expect(dependencyMocks.execa).toHaveBeenCalledWith(layout.executable, ['-U'], {
        timeout: 120_000,
      });
      expect(dependencyMocks.execa).toHaveBeenLastCalledWith(layout.executable, ['--version'], {
        timeout: 15_000,
      });
    } finally {
      await fs.rm(layout.root, {recursive: true, force: true});
    }
  });

  it('keeps an installed version nonfatal after both update mechanisms fail', async () => {
    const layout = await makeExecutableLayout();
    const pythonExecutable = await fs.realpath(layout.python);
    process.env.YT_DLP_PATH = layout.executable;
    dependencyMocks.execa.mockImplementation(async (executable: string, args: string[]) => {
      if (args[0] === '--version') {
        return {stdout: '2026.01.01'};
      }

      if (executable === pythonExecutable) {
        throw {stderr: 'pip update failed'};
      }

      if (args[0] === '-U') {
        throw {stderr: 'self update failed'};
      }

      throw new Error(`Unexpected command: ${executable} ${args.join(' ')}`);
    });
    const log = vi.spyOn(console, 'log').mockImplementation(() => undefined);
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);

    try {
      await expect(prepareYtDlp({YT_DLP_AUTO_UPDATE: true} as never)).resolves.toBeUndefined();
      expect(warn).toHaveBeenCalledWith('yt-dlp update warning: pip update failed; self update failed');
      expect(log).toHaveBeenCalledWith(
        'YT_DLP_VERSION=2026.01.01 (update failed; continuing with installed version)',
      );
      expect(dependencyMocks.execa.mock.calls.filter(([, args]) => (args as string[])[0] === '--version')).toHaveLength(2);
    } finally {
      await fs.rm(layout.root, {recursive: true, force: true});
    }
  });
});

describe('OPS-11 yt-dlp extraction and ffmpeg handoff', () => {
  it('bounds SoundCloud collection extraction and does not read YouTube cookies', async () => {
    process.env.YT_DLP_COOKIES_PATH = '/missing/youtube-only-cookies';
    const metadata = {title: 'Album', entries: [{title: 'Track', url: 'https://soundcloud.com/artist/track'}]};
    dependencyMocks.execa.mockResolvedValue({stdout: JSON.stringify(metadata)});
    await expect(getSoundCloudMetadata('https://soundcloud.com/artist/sets/album', 3)).resolves.toEqual(metadata);
    const args = dependencyMocks.execa.mock.calls[0][1] as string[];
    expect(args).toEqual(expect.arrayContaining(['--flat-playlist', '--playlist-end', '3']));
    expect(args).not.toContain('--cookies');
    expect(dependencyMocks.execa.mock.calls[0][2]).toEqual({timeout: 45_000});
  });

  it('rejects collection results when resolving an individual SoundCloud audio stream', async () => {
    dependencyMocks.execa.mockResolvedValue({stdout: JSON.stringify({entries: [], url: 'https://soundcloud.com/artist'})});
    await expect(getSoundCloudMediaSource('https://soundcloud.com/artist')).rejects.toThrow('playable media URL');
  });

  it.each([
    'ERROR: [soundcloud] 123: This video is DRM protected',
    'ERROR: [soundcloud] artist/removed: Unable to download JSON metadata: HTTP Error 404: Not Found',
  ])('classifies permanent SoundCloud extraction failure as unplayable: %s', async stderr => {
    dependencyMocks.execa.mockRejectedValue({stderr});
    await expect(getSoundCloudMediaSource('https://soundcloud.com/artist/protected'))
      .rejects.toBeInstanceOf(YtDlpMediaUnavailableError);
  });

  it('preserves temporary SoundCloud failures for retry', async () => {
    dependencyMocks.execa.mockRejectedValue({stderr: 'ERROR: [soundcloud] 123: HTTP Error 429: Too Many Requests'});
    await expect(getSoundCloudMediaSource('https://soundcloud.com/artist/track'))
      .rejects.not.toBeInstanceOf(YtDlpMediaUnavailableError);
  });

  it('uses the bounded single-video Node-runtime extraction contract and normalizes the selected download', async () => {
    process.env.YT_DLP_PATH = '/fake/yt-dlp';

    await expect(getYouTubeMediaSource('abcdefghijk')).resolves.toEqual({
      url: 'https://media.example/requested.webm',
      headers: {
        Authorization: 'Bearer media-token',
        'User-Agent': 'Muse ops test',
      },
      isLive: true,
    });
    expect(dependencyMocks.execa).toHaveBeenCalledWith('/fake/yt-dlp', [
      '--dump-single-json',
      '--no-playlist',
      '--skip-download',
      '--no-warnings',
      '--no-cache-dir',
      '--js-runtimes',
      'node',
      '-f',
      'bestaudio/best',
      '-S',
      'proto:https',
      'https://www.youtube.com/watch?v=abcdefghijk',
    ], {
      timeout: 45_000,
    });
  });

  it.each([
    [MediaSource.Youtube, 'abcdefghijk', 'https://www.youtube.com/watch?v=abcdefghijk'],
    [MediaSource.SoundCloud, 'https://soundcloud.com/artist/track', 'https://soundcloud.com/artist/track'],
  ])('hands fresh media and normalized headers to ffmpeg for source %s', async (source, url, extractionUrl) => {
    process.env.YT_DLP_PATH = '/fake/yt-dlp';
    const fileCache = {
      getEntryFor: vi.fn().mockResolvedValue(null),
    };
    const player = new Player(fileCache as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'Long uncached track',
      artist: 'Artist',
      url: url as string,
      length: 3_600,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: source as MediaSource,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    const stream = await getOwnedStream(player, song);
    const command = dependencyMocks.ffmpeg.mock.results[0].value as ReturnType<typeof makeFfmpegCommand>;

    expect(dependencyMocks.ffmpeg).toHaveBeenCalledWith('https://media.example/requested.webm');
    expect(dependencyMocks.execa.mock.calls[0][1].at(-1)).toBe(extractionUrl);
    expect(command.inputOptions).toHaveBeenCalledWith([
      '-reconnect',
      '1',
      '-reconnect_streamed',
      '1',
      '-reconnect_delay_max',
      '5',
      '-headers',
      'Authorization: Bearer media-token\r\nUser-Agent: Muse ops test\r\n',
    ]);
    expect(command.noVideo).toHaveBeenCalledOnce();
    expect(command.audioCodec).toHaveBeenCalledWith('libopus');
    expect(command.outputFormat).toHaveBeenCalledWith('webm');

    stream.destroy();
    await flushAsyncWork();
  });

  it('rejects an FFmpeg startup failure before reporting a playable stream and redacts the signed URL', async () => {
    process.env.YT_DLP_PATH = '/fake/yt-dlp';
    const signedUrl = 'HTTPS://media.example/requested.webm?token=secret';
    dependencyMocks.ffmpeg.mockImplementation(() => makeFfmpegCommand({
      startupError: new Error(`${signedUrl}: Server returned 404 Not Found`),
    }));
    const fileCache = {
      getEntryFor: vi.fn().mockResolvedValue(null),
    };
    const player = new Player(fileCache as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'Rejected media URL',
      artist: 'Artist',
      url: 'abcdefghijk',
      length: 3_600,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: MediaSource.Youtube,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    const result = getOwnedStream(player, song);

    await expect(result).rejects.toEqual(expect.objectContaining({
      name: 'FfmpegMediaUnavailableError',
      message: expect.stringContaining('[media URL]'),
    }));
    await expect(result).rejects.not.toEqual(expect.objectContaining({
      message: expect.stringContaining('token=secret'),
    }));
    await expect(result).rejects.toBeInstanceOf(FfmpegMediaUnavailableError);
  });

  it('rejects an empty FFmpeg output instead of reporting playback', async () => {
    dependencyMocks.ffmpeg.mockImplementation(() => makeFfmpegCommand({emptyOutput: true}));
    const player = new Player({
      getEntryFor: vi.fn().mockResolvedValue({generation: 'cached-generation', path: '/cached/audio.webm'}),
    } as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'Empty media output',
      artist: 'Artist',
      url: 'https://media.example/empty.webm',
      length: 100,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: MediaSource.HLS,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    const result = getOwnedStream(player, song);
    await expect(result).rejects.toMatchObject({
      name: 'FfmpegMediaUnavailableError',
      message: expect.stringContaining('produced no playable Opus audio'),
    });
  });

  it('preserves a startup timeout for retry instead of skipping the queue', async () => {
    vi.useFakeTimers();
    dependencyMocks.ffmpeg.mockImplementation(() => makeFfmpegCommand({hangOutput: true}));
    const player = new Player({
      getEntryFor: vi.fn().mockResolvedValue({generation: 'cached-generation', path: '/cached/audio.webm'}),
    } as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'Slow startup',
      artist: 'Artist',
      url: 'abcdefghijk',
      length: 100,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: MediaSource.Youtube,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    const result = getOwnedStream(player, song);
    const expectation = expect(result).rejects.toMatchObject({
      name: 'FfmpegStartupError',
      message: expect.stringContaining('within 20 seconds'),
    });

    await vi.advanceTimersByTimeAsync(20_000);
    await expectation;

    const command = dependencyMocks.ffmpeg.mock.results[0].value as ReturnType<typeof makeFfmpegCommand>;
    const killsBeforeLateStart = command.kill.mock.calls.length;
    command.triggerStart();
    expect(command.kill).toHaveBeenCalledTimes(killsBeforeLateStart + 1);
    expect(command.kill).toHaveBeenLastCalledWith('SIGKILL');
  });

  it('waits for the FFmpeg result before classifying output EOF', async () => {
    dependencyMocks.ffmpeg.mockImplementation(() => makeFfmpegCommand({
      emptyOutput: true,
      startupErrorAfterOutput: new Error('Server returned 403 Forbidden'),
    }));
    const player = new Player({
      getEntryFor: vi.fn().mockResolvedValue({generation: 'cached-generation', path: '/cached/audio.webm'}),
    } as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'EOF before process result',
      artist: 'Artist',
      url: 'abcdefghijk',
      length: 100,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: MediaSource.Youtube,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    await expect(getOwnedStream(player, song)).rejects.toBeInstanceOf(FfmpegStartupError);
  });

  it('does not accept a container header without a playable Opus packet', async () => {
    dependencyMocks.ffmpeg.mockImplementation(() => makeFfmpegCommand({
      headerOnlyOutput: true,
    }));
    const player = new Player({
      getEntryFor: vi.fn().mockResolvedValue({generation: 'cached-generation', path: '/cached/audio.webm'}),
    } as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'Header-only output',
      artist: 'Artist',
      url: 'https://media.example/header-only.webm',
      length: 100,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: MediaSource.HLS,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    await expect(getOwnedStream(player, song)).rejects.toBeInstanceOf(FfmpegMediaUnavailableError);
  });

  it('invalidates a corrupt cached input and retries from a fresh media URL', async () => {
    dependencyMocks.ffmpeg
      .mockImplementationOnce(() => makeFfmpegCommand({headerOnlyOutput: true}))
      .mockImplementationOnce(() => makeFfmpegCommand());
    const invalidate = vi.fn().mockResolvedValue(undefined);
    const player = new Player({
      getEntryFor: vi.fn()
        .mockResolvedValueOnce({generation: 'corrupt-generation', path: '/cached/corrupt.webm'})
        .mockResolvedValueOnce(null),
      invalidate,
    } as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'Corrupt cached input',
      artist: 'Artist',
      url: 'abcdefghijk',
      length: 3_600,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: MediaSource.Youtube,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    const stream = await getOwnedStream(player, song);

    expect(invalidate).toHaveBeenCalledWith(expect.any(String), 'corrupt-generation');
    expect(dependencyMocks.ffmpeg).toHaveBeenNthCalledWith(1, '/cached/corrupt.webm');
    expect(dependencyMocks.ffmpeg).toHaveBeenNthCalledWith(2, 'https://media.example/requested.webm');
    stream.destroy();
  });

  it('preserves infrastructure-level FFmpeg startup failures', async () => {
    dependencyMocks.ffmpeg.mockImplementation(() => makeFfmpegCommand({
      startupError: new Error('spawn ffmpeg ENOENT'),
    }));
    const player = new Player({
      getEntryFor: vi.fn().mockResolvedValue({generation: 'cached-generation', path: '/cached/audio.webm'}),
    } as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'Host failure',
      artist: 'Artist',
      url: 'abcdefghijk',
      length: 100,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: MediaSource.Youtube,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    await expect(getOwnedStream(player, song)).rejects.toBeInstanceOf(FfmpegStartupError);
  });

  it('preserves transient HTTP failures for retry instead of skipping the queue', async () => {
    dependencyMocks.ffmpeg.mockImplementation(() => makeFfmpegCommand({
      startupError: new Error('Server returned 403 Forbidden'),
    }));
    const player = new Player({
      getEntryFor: vi.fn().mockResolvedValue({generation: 'cached-generation', path: '/cached/audio.webm'}),
    } as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'Transient media failure',
      artist: 'Artist',
      url: 'abcdefghijk',
      length: 100,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: MediaSource.Youtube,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    await expect(getOwnedStream(player, song)).rejects.toBeInstanceOf(FfmpegStartupError);
  });

  it('destroys an in-progress cache destination when FFmpeg fails during startup', async () => {
    dependencyMocks.execa.mockResolvedValue({
      stdout: JSON.stringify({
        ...JSON.parse(VALID_MEDIA_RESPONSE),
        is_live: false,
      }),
    });
    dependencyMocks.ffmpeg.mockImplementation(() => makeFfmpegCommand({
      startupError: new Error('Server returned 404 Not Found'),
    }));
    const cacheDestination = new PassThrough();
    const destroyCacheDestination = vi.spyOn(cacheDestination, 'destroy');
    const player = new Player({
      createWriteStream: vi.fn(() => cacheDestination),
      getEntryFor: vi.fn().mockResolvedValue(null),
    } as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'Cacheable rejected media',
      artist: 'Artist',
      url: 'abcdefghijk',
      length: 100,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: MediaSource.Youtube,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    await expect(getOwnedStream(player, song)).rejects.toBeInstanceOf(FfmpegMediaUnavailableError);
    expect(destroyCacheDestination).toHaveBeenCalledOnce();
  });

  it('finalizes a cache write only after FFmpeg exits successfully', async () => {
    dependencyMocks.execa.mockResolvedValue({
      stdout: JSON.stringify({
        ...JSON.parse(VALID_MEDIA_RESPONSE),
        is_live: false,
      }),
    });
    const cacheDestination = new PassThrough();
    const endCacheDestination = vi.spyOn(cacheDestination, 'end');
    const player = new Player({
      createWriteStream: vi.fn(() => cacheDestination),
      getEntryFor: vi.fn().mockResolvedValue(null),
    } as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'Successful cache media',
      artist: 'Artist',
      url: 'abcdefghijk',
      length: 100,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: MediaSource.Youtube,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    const stream = await getOwnedStream(player, song);
    await vi.waitFor(() => {
      expect(endCacheDestination).toHaveBeenCalledOnce();
    });
    stream.destroy();
  });

  it('discards an in-progress cache if FFmpeg fails after the first playable packet', async () => {
    dependencyMocks.ffmpeg.mockImplementation(() => makeFfmpegCommand({deferProcessResult: true}));
    dependencyMocks.execa.mockResolvedValue({
      stdout: JSON.stringify({
        ...JSON.parse(VALID_MEDIA_RESPONSE),
        is_live: false,
      }),
    });
    const cacheDestination = new PassThrough();
    const destroyCacheDestination = vi.spyOn(cacheDestination, 'destroy');
    const player = new Player({
      createWriteStream: vi.fn(() => cacheDestination),
      getEntryFor: vi.fn().mockResolvedValue(null),
    } as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'Partially playable media',
      artist: 'Artist',
      url: 'abcdefghijk',
      length: 100,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: MediaSource.Youtube,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    const stream = await getOwnedStream(player, song);
    const command = dependencyMocks.ffmpeg.mock.results[0].value as ReturnType<typeof makeFfmpegCommand>;
    command.triggerError(new Error('Server returned 403 Forbidden'));
    await flushAsyncWork();

    expect(destroyCacheDestination).toHaveBeenCalledOnce();
    stream.destroy();
  });

  it('discards an in-progress cache if FFmpeg fails after the playback reader closes', async () => {
    dependencyMocks.ffmpeg.mockImplementation(() => makeFfmpegCommand({deferProcessResult: true}));
    dependencyMocks.execa.mockResolvedValue({
      stdout: JSON.stringify({
        ...JSON.parse(VALID_MEDIA_RESPONSE),
        is_live: false,
      }),
    });
    const cacheDestination = new PassThrough();
    const destroyCacheDestination = vi.spyOn(cacheDestination, 'destroy');
    const player = new Player({
      createWriteStream: vi.fn(() => cacheDestination),
      getEntryFor: vi.fn().mockResolvedValue(null),
    } as never, GUILD_ID);
    const song: QueuedSong = {
      title: 'Stopped partial media',
      artist: 'Artist',
      url: 'abcdefghijk',
      length: 100,
      offset: 0,
      playlist: null,
      isLive: false,
      thumbnailUrl: null,
      source: MediaSource.Youtube,
      addedInChannelId: 'text-channel-id',
      requestedBy: 'requester-id',
    };

    const stream = await getOwnedStream(player, song);
    const command = dependencyMocks.ffmpeg.mock.results[0].value as ReturnType<typeof makeFfmpegCommand>;
    stream.destroy();
    command.triggerError(new Error('Server returned 403 Forbidden'));
    await flushAsyncWork();

    expect(destroyCacheDestination).toHaveBeenCalledOnce();
  });
});

describe('OPS-12 optional Spotify bindings', () => {
  it('binds both Spotify services and advertises Spotify only when both credentials are nonblank', async () => {
    const {container, Play, TYPES} = await loadSpotifyBindingHarness(' client-id ', ' client-secret ');
    const command = new Play({spotify: {}} as never, {} as never, {} as never);
    const queryOption = command.slashCommand.toJSON().options?.find(option => option.name === 'query');

    expect(container.isBound(TYPES.Services.SpotifyAPI)).toBe(true);
    expect(container.isBound(TYPES.ThirdParty)).toBe(true);
    expect(container.isBound(TYPES.Services.YoutubeAPI)).toBe(true);
    expect(container.isBound(TYPES.Services.GetSongs)).toBe(true);
    expect(queryOption?.description).toBe('YouTube URL, Spotify URL, or search query');
  });

  it.each([
    ['', 'client-secret', 'missing ID'],
    ['client-id', '', 'missing secret'],
    ['   ', 'client-secret', 'blank ID'],
    ['client-id', ' \t ', 'blank secret'],
  ])('keeps core YouTube services and description when credentials have %s / %s (%s)', async (
    clientId,
    clientSecret,
  ) => {
    const {container, Play, TYPES} = await loadSpotifyBindingHarness(clientId, clientSecret);
    const command = new Play(undefined as never, {} as never, {} as never);
    const queryOption = command.slashCommand.toJSON().options?.find(option => option.name === 'query');

    expect(container.isBound(TYPES.Services.SpotifyAPI)).toBe(false);
    expect(container.isBound(TYPES.ThirdParty)).toBe(false);
    expect(container.isBound(TYPES.Services.YoutubeAPI)).toBe(true);
    expect(container.isBound(TYPES.Services.GetSongs)).toBe(true);
    expect(queryOption?.description).toBe('YouTube URL or search query');
  });
});

describe('OPS-12 Spotify token lifecycle', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  it('installs the token, refreshes at half-life, and cleans up the pending timer', async () => {
    dependencyMocks.spotifyClientCredentialsGrant
      .mockResolvedValueOnce({body: {access_token: 'token-one', expires_in: 120}})
      .mockResolvedValueOnce({body: {access_token: 'token-two', expires_in: 240}});
    const setTimeoutSpy = vi.spyOn(global, 'setTimeout');
    const service = new ThirdParty({
      SPOTIFY_CLIENT_ID: 'client-id',
      SPOTIFY_CLIENT_SECRET: 'client-secret',
    } as never);
    await flushAsyncWork();

    expect(dependencyMocks.spotifyConstructorOptions).toEqual([{
      clientId: 'client-id',
      clientSecret: 'client-secret',
    }]);
    expect(dependencyMocks.spotifySetAccessToken).toHaveBeenNthCalledWith(1, 'token-one');
    expect(setTimeoutSpy).toHaveBeenLastCalledWith(expect.any(Function), 60_000);
    expect(vi.getTimerCount()).toBe(1);

    await vi.advanceTimersByTimeAsync(60_000);
    await flushAsyncWork();

    expect(dependencyMocks.spotifyClientCredentialsGrant).toHaveBeenCalledTimes(2);
    expect(dependencyMocks.spotifySetAccessToken).toHaveBeenNthCalledWith(2, 'token-two');
    expect(setTimeoutSpy).toHaveBeenLastCalledWith(expect.any(Function), 120_000);
    expect(vi.getTimerCount()).toBe(1);

    service.cleanup();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('bounds credential-grant retry to five retries before a successful sixth attempt', async () => {
    for (let attempt = 0; attempt < 5; attempt++) {
      dependencyMocks.spotifyClientCredentialsGrant.mockRejectedValueOnce(new Error(`attempt ${attempt + 1}`));
    }

    dependencyMocks.spotifyClientCredentialsGrant.mockResolvedValueOnce({
      body: {access_token: 'eventual-token', expires_in: 120},
    });
    const service = new ThirdParty({
      SPOTIFY_CLIENT_ID: 'client-id',
      SPOTIFY_CLIENT_SECRET: 'client-secret',
    } as never);
    await flushAsyncWork();

    await vi.advanceTimersByTimeAsync(31_000);
    await flushAsyncWork();

    expect(dependencyMocks.spotifyClientCredentialsGrant).toHaveBeenCalledTimes(6);
    expect(dependencyMocks.spotifySetAccessToken).toHaveBeenCalledOnce();
    expect(dependencyMocks.spotifySetAccessToken).toHaveBeenCalledWith('eventual-token');
    expect(vi.getTimerCount()).toBe(1);

    service.cleanup();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('logs exhausted refresh failure and reschedules one attempt after 60 seconds', async () => {
    dependencyMocks.spotifyClientCredentialsGrant.mockRejectedValue(new Error('Spotify auth unavailable'));
    const setTimeoutSpy = vi.spyOn(global, 'setTimeout');
    const service = new ThirdParty({
      SPOTIFY_CLIENT_ID: 'client-id',
      SPOTIFY_CLIENT_SECRET: 'client-secret',
    } as never);
    await flushAsyncWork();

    await vi.advanceTimersByTimeAsync(31_000);
    await flushAsyncWork();

    expect(dependencyMocks.spotifyClientCredentialsGrant).toHaveBeenCalledTimes(6);
    expect(dependencyMocks.spotifySetAccessToken).not.toHaveBeenCalled();
    expect(dependencyMocks.debug).toHaveBeenCalledWith(
      'Spotify token refresh failed: %O',
      expect.objectContaining({message: 'Spotify auth unavailable'}),
    );
    expect(setTimeoutSpy).toHaveBeenLastCalledWith(expect.any(Function), 60_000);
    expect(vi.getTimerCount()).toBe(1);

    service.cleanup();
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe('OPS-13 transient voice recovery', () => {
  it.each(['connecting', 'signalling'])('keeps a 4014 connection when it recovers through %s', async recoveredStatus => {
    const never = new Promise<void>(() => undefined);
    dependencyMocks.entersState.mockImplementation(async (_connection, status: string) => (
      status === recoveredStatus ? undefined : never
    ));
    const {connection} = makeVoiceConnection({
      state: {
        status: 'disconnected',
        reason: 'websocket-close',
        closeCode: 4014,
      },
    });
    const player = new Player({} as never, GUILD_ID);
    player.voiceConnection = connection as never;

    await getPrivatePlayer(player).onVoiceConnectionDisconnect(connection);

    expect(dependencyMocks.entersState).toHaveBeenCalledWith(connection, 'connecting', 5_000);
    expect(dependencyMocks.entersState).toHaveBeenCalledWith(connection, 'signalling', 5_000);
    expect(connection.destroy).not.toHaveBeenCalled();
    expect(player.voiceConnection).toBe(connection);
  });

  it('destroys a 4014 connection when both five-second recovery waits time out', async () => {
    dependencyMocks.entersState.mockRejectedValue(new Error('recovery timed out'));
    const {connection} = makeVoiceConnection({
      state: {
        status: 'disconnected',
        reason: 'websocket-close',
        closeCode: 4014,
      },
    });
    const player = new Player({} as never, GUILD_ID);
    player.voiceConnection = connection as never;

    await getPrivatePlayer(player).onVoiceConnectionDisconnect(connection);

    expect(dependencyMocks.entersState).toHaveBeenCalledWith(connection, 'connecting', 5_000);
    expect(dependencyMocks.entersState).toHaveBeenCalledWith(connection, 'signalling', 5_000);
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(player.voiceConnection).toBeNull();
  });

  it('does not disconnect a replacement when an old 4014 recovery wait later times out', async () => {
    const oldRecovery = makeDeferred<void>();
    dependencyMocks.entersState.mockReturnValue(oldRecovery.promise);
    const {connection: oldConnection} = makeVoiceConnection({
      state: {
        status: 'disconnected',
        reason: 'websocket-close',
        closeCode: 4014,
      },
    });
    const {connection: replacement} = makeVoiceConnection({
      state: {status: 'disconnected'},
    });
    const player = new Player({} as never, GUILD_ID);
    player.voiceConnection = oldConnection as never;

    const recovery = getPrivatePlayer(player).onVoiceConnectionDisconnect(oldConnection);
    await flushAsyncWork();
    player.voiceConnection = replacement as never;
    oldRecovery.reject(new Error('old 4014 recovery timed out'));
    await recovery;

    expect(oldConnection.destroy).toHaveBeenCalledOnce();
    expect(replacement.rejoin).not.toHaveBeenCalled();
    expect(replacement.destroy).not.toHaveBeenCalled();
    expect(player.voiceConnection).toBe(replacement);
  });

  it('cleans up an old 4014 connection that recovers after it was replaced', async () => {
    const oldRecovery = makeDeferred<void>();
    dependencyMocks.entersState.mockReturnValue(oldRecovery.promise);
    const {connection: oldConnection} = makeVoiceConnection({
      state: {
        status: 'disconnected',
        reason: 'websocket-close',
        closeCode: 4014,
      },
    });
    const {connection: replacement} = makeVoiceConnection({
      state: {status: 'disconnected'},
    });
    const player = new Player({} as never, GUILD_ID);
    player.voiceConnection = oldConnection as never;

    const recovery = getPrivatePlayer(player).onVoiceConnectionDisconnect(oldConnection);
    await flushAsyncWork();
    player.voiceConnection = replacement as never;
    oldRecovery.resolve(undefined);
    await recovery;

    expect(oldConnection.destroy).toHaveBeenCalledOnce();
    expect(replacement.rejoin).not.toHaveBeenCalled();
    expect(replacement.destroy).not.toHaveBeenCalled();
    expect(player.voiceConnection).toBe(replacement);
  });

  it('does not destroy an already Destroyed stale connection or touch its replacement', async () => {
    const {connection: oldConnection} = makeVoiceConnection({
      state: {status: 'destroyed'},
    });
    const {connection: replacement} = makeVoiceConnection({
      state: {status: 'disconnected'},
    });
    const player = new Player({} as never, GUILD_ID);
    player.voiceConnection = replacement as never;

    await getPrivatePlayer(player).onVoiceConnectionDisconnect(oldConnection);

    expect(oldConnection.destroy).not.toHaveBeenCalled();
    expect(replacement.rejoin).not.toHaveBeenCalled();
    expect(replacement.destroy).not.toHaveBeenCalled();
    expect(player.voiceConnection).toBe(replacement);
  });

  it('backs off in five-second increments and rejoins below five attempts', async () => {
    const backoff = makeDeferred<void>();
    dependencyMocks.sleep.mockReturnValue(backoff.promise);
    const {connection} = makeVoiceConnection({
      rejoinAttempts: 2,
      state: {status: 'disconnected'},
    });
    const player = new Player({} as never, GUILD_ID);
    player.voiceConnection = connection as never;

    const recovery = getPrivatePlayer(player).onVoiceConnectionDisconnect(connection);
    expect(dependencyMocks.sleep).toHaveBeenCalledWith(15_000);
    backoff.resolve(undefined);
    await recovery;

    expect(connection.rejoin).toHaveBeenCalledOnce();
    expect(connection.destroy).not.toHaveBeenCalled();
    expect(player.voiceConnection).toBe(connection);
  });

  it('does not destroy a connection that becomes Ready during disconnect backoff', async () => {
    const backoff = makeDeferred<void>();
    dependencyMocks.sleep.mockReturnValue(backoff.promise);
    const {connection} = makeVoiceConnection({
      rejoinAttempts: 1,
      state: {status: 'disconnected'},
    });
    const player = new Player({} as never, GUILD_ID);
    player.voiceConnection = connection as never;

    const recovery = getPrivatePlayer(player).onVoiceConnectionDisconnect(connection);
    expect(dependencyMocks.sleep).toHaveBeenCalledWith(10_000);

    connection.state.status = 'ready';
    backoff.resolve(undefined);
    await recovery;

    expect(connection.rejoin).not.toHaveBeenCalled();
    expect(connection.destroy).not.toHaveBeenCalled();
    expect(player.voiceConnection).toBe(connection);
  });

  it('clears a current connection that becomes Destroyed during transient backoff without destroying it again', async () => {
    const backoff = makeDeferred<void>();
    dependencyMocks.sleep.mockReturnValue(backoff.promise);
    const {connection} = makeVoiceConnection({
      rejoinAttempts: 1,
      state: {status: 'disconnected'},
    });
    const player = new Player({} as never, GUILD_ID);
    const audioPlayer = {stop: vi.fn()};
    player.voiceConnection = connection as never;
    Object.assign(player, {
      audioPlayer,
      audioResource: {volume: {}},
      currentChannel: {id: 'old-channel'},
    });

    const recovery = getPrivatePlayer(player).onVoiceConnectionDisconnect(connection);
    connection.state.status = 'destroyed';
    backoff.resolve(undefined);
    await recovery;

    const state = player as unknown as {
      audioPlayer: object | null;
      audioResource: object | null;
      currentChannel?: object;
    };
    expect(connection.destroy).not.toHaveBeenCalled();
    expect(connection.rejoin).not.toHaveBeenCalled();
    expect(audioPlayer.stop).toHaveBeenCalledWith(true);
    expect(player.voiceConnection).toBeNull();
    expect(state.audioPlayer).toBeNull();
    expect(state.audioResource).toBeNull();
    expect(state.currentChannel).toBeUndefined();
  });

  it('does not rejoin or destroy a replacement when an old transient backoff later releases', async () => {
    const backoff = makeDeferred<void>();
    dependencyMocks.sleep.mockReturnValue(backoff.promise);
    const {connection: oldConnection} = makeVoiceConnection({
      rejoinAttempts: 1,
      state: {status: 'disconnected'},
    });
    const {connection: replacement} = makeVoiceConnection({
      rejoinAttempts: 0,
      state: {status: 'disconnected'},
    });
    const player = new Player({} as never, GUILD_ID);
    player.voiceConnection = oldConnection as never;

    const recovery = getPrivatePlayer(player).onVoiceConnectionDisconnect(oldConnection);
    expect(dependencyMocks.sleep).toHaveBeenCalledWith(10_000);
    player.voiceConnection = replacement as never;
    backoff.resolve(undefined);
    await recovery;

    expect(oldConnection.rejoin).not.toHaveBeenCalled();
    expect(oldConnection.destroy).toHaveBeenCalledOnce();
    expect(replacement.rejoin).not.toHaveBeenCalled();
    expect(replacement.destroy).not.toHaveBeenCalled();
    expect(player.voiceConnection).toBe(replacement);
  });

  it('clears a current 4014 connection already Destroyed when its recovery wait rejects', async () => {
    const oldRecovery = makeDeferred<void>();
    dependencyMocks.entersState.mockReturnValue(oldRecovery.promise);
    const {connection} = makeVoiceConnection({
      state: {
        status: 'disconnected',
        reason: 'websocket-close',
        closeCode: 4014,
      },
    });
    const player = new Player({} as never, GUILD_ID);
    const audioPlayer = {stop: vi.fn()};
    player.voiceConnection = connection as never;
    Object.assign(player, {
      audioPlayer,
      audioResource: {volume: {}},
      currentChannel: {id: 'old-channel'},
    });

    const recovery = getPrivatePlayer(player).onVoiceConnectionDisconnect(connection);
    await flushAsyncWork();
    connection.state.status = 'destroyed';
    oldRecovery.reject(new Error('4014 recovery ended after destruction'));
    await recovery;

    const state = player as unknown as {
      audioPlayer: object | null;
      audioResource: object | null;
      currentChannel?: object;
    };
    expect(connection.destroy).not.toHaveBeenCalled();
    expect(audioPlayer.stop).toHaveBeenCalledWith(true);
    expect(player.voiceConnection).toBeNull();
    expect(state.audioPlayer).toBeNull();
    expect(state.audioResource).toBeNull();
    expect(state.currentChannel).toBeUndefined();
  });

  it('destroys a still-disconnected connection when rejoin fails below five attempts', async () => {
    const {connection} = makeVoiceConnection({
      rejoin: vi.fn(() => false),
      rejoinAttempts: 0,
      state: {status: 'disconnected'},
    });
    const player = new Player({} as never, GUILD_ID);
    player.voiceConnection = connection as never;

    await getPrivatePlayer(player).onVoiceConnectionDisconnect(connection);

    expect(dependencyMocks.sleep).toHaveBeenCalledWith(5_000);
    expect(connection.rejoin).toHaveBeenCalledOnce();
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(player.voiceConnection).toBeNull();
  });

  it('destroys immediately when rejoin attempts are exhausted', async () => {
    const {connection} = makeVoiceConnection({
      rejoinAttempts: 5,
      state: {status: 'disconnected'},
    });
    const player = new Player({} as never, GUILD_ID);
    player.voiceConnection = connection as never;

    await getPrivatePlayer(player).onVoiceConnectionDisconnect(connection);

    expect(dependencyMocks.sleep).not.toHaveBeenCalled();
    expect(connection.rejoin).not.toHaveBeenCalled();
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(player.voiceConnection).toBeNull();
  });

  it('keeps a newer connection when an older initial Ready wait rejects and ignores the old Ready callback', async () => {
    const readyA = makeDeferred<void>();
    const readyB = makeDeferred<void>();
    const {connection: connectionA, handlers: handlersA} = makeVoiceConnection({
      rejoinAttempts: 2,
      state: {status: 'connecting'},
    });
    const {connection: connectionB} = makeVoiceConnection({
      state: {status: 'connecting'},
    });
    dependencyMocks.joinVoiceChannel
      .mockReturnValueOnce(connectionA)
      .mockReturnValueOnce(connectionB);
    dependencyMocks.entersState.mockImplementation((_connection: unknown) => (
      _connection === connectionA ? readyA.promise : readyB.promise
    ));
    const player = new Player({} as never, GUILD_ID);
    const registerVoiceActivityListener = vi.spyOn(player, 'registerVoiceActivityListener');
    const channelA = {
      id: 'voice-channel-a',
      guild: {id: GUILD_ID, voiceAdapterCreator: {}},
    };
    const channelB = {
      id: 'voice-channel-b',
      guild: {id: GUILD_ID, voiceAdapterCreator: {}},
    };

    const connectingA = player.connect(channelA as never);
    await flushAsyncWork();
    const connectingB = player.connect(channelB as never);
    await flushAsyncWork();
    expect(player.voiceConnection).toBe(connectionB);

    handlersA.get('stateChange')!({status: 'connecting'}, {status: 'ready'});
    connectionA.state.status = 'disconnected';
    readyA.reject(new Error('connection A Ready timeout'));
    await expect(connectingA).rejects.toThrow(
      'Failed to connect to the voice channel (last state: disconnected, network state: unavailable, attempt: 1/3, rejoin attempts: 2, recent states: connecting -> ready).',
    );

    readyB.resolve(undefined);
    await connectingB;

    expect(connectionA.destroy).toHaveBeenCalled();
    expect(connectionB.destroy).not.toHaveBeenCalled();
    expect(player.voiceConnection).toBe(connectionB);
    expect(registerVoiceActivityListener).not.toHaveBeenCalled();
  });

  it('uses bounded initial Ready waits and preserves diagnostics without retrying a disconnect', async () => {
    const ready = makeDeferred<void>();
    dependencyMocks.entersState.mockReturnValue(ready.promise);
    const {connection, handlers} = makeVoiceConnection({
      rejoinAttempts: 3,
      state: {status: 'connecting'},
    });
    dependencyMocks.joinVoiceChannel.mockReturnValue(connection);
    const player = new Player({} as never, GUILD_ID);
    const channel = {
      id: 'voice-channel-id',
      guild: {
        id: GUILD_ID,
        voiceAdapterCreator: {},
      },
    };

    const connecting = player.connect(channel as never);
    await flushAsyncWork();
    expect(dependencyMocks.entersState).toHaveBeenCalledWith(connection, 'ready', 20_000);

    handlers.get('stateChange')!({status: 'connecting'}, {status: 'signalling'});
    handlers.get('stateChange')!({status: 'signalling'}, {status: 'disconnected'});
    connection.state.status = 'disconnected';
    ready.reject(new Error('initial Ready timeout'));

    await expect(connecting).rejects.toThrow(
      'Failed to connect to the voice channel (last state: disconnected, network state: unavailable, attempt: 1/3, rejoin attempts: 3, recent states: connecting -> signalling -> disconnected).',
    );
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(player.voiceConnection).toBeNull();
  });
});


describe('Initial voice handshake retries', () => {
  const channel = {id: 'voice-channel-id', guild: {id: GUILD_ID, voiceAdapterCreator: {}}};
  const makeInitialConnection = (status = 'connecting', networkCode = 0) => {
    const {connection, handlers} = makeVoiceConnection({
      state: {status, networking: {state: {code: networkCode, connectionOptions: {token: 'private-voice-token'}}}},
    });
    connection.destroy.mockImplementation(() => {
      const oldState = connection.state;
      connection.state = {status: 'destroyed'};
      handlers.get('stateChange')?.(oldState, connection.state);
    });
    return connection;
  };

  beforeEach(() => {
    dependencyMocks.joinVoiceChannel.mockReset();
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
  });

  it.each(['connecting', 'signalling'])('replaces a stalled %s handshake and succeeds on the next join', async status => {
    const first = makeInitialConnection(status);
    const second = makeInitialConnection();
    dependencyMocks.joinVoiceChannel.mockReturnValueOnce(first).mockReturnValueOnce(second);
    dependencyMocks.entersState.mockRejectedValueOnce(new Error('timeout')).mockResolvedValueOnce(undefined);
    const player = new Player({} as never, GUILD_ID);

    await player.connect(channel as never);

    expect(dependencyMocks.joinVoiceChannel).toHaveBeenCalledTimes(2);
    expect(first.destroy).toHaveBeenCalledOnce();
    expect(first.rejoin).not.toHaveBeenCalled();
    expect(second.destroy).not.toHaveBeenCalled();
    expect(player.voiceConnection).toBe(second);
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('network state: OpeningWs'));
    expect(JSON.stringify(vi.mocked(console.warn).mock.calls)).not.toContain('private-voice-token');
  });

  it('bounds repeated failures at three attempts and snapshots diagnostics before destruction', async () => {
    const connections = Array.from({length: 3}, () => makeInitialConnection('connecting', 2));
    connections.forEach(connection => dependencyMocks.joinVoiceChannel.mockReturnValueOnce(connection));
    dependencyMocks.entersState.mockRejectedValue(new Error('timeout'));
    const player = new Player({} as never, GUILD_ID);

    await expect(player.connect(channel as never)).rejects.toThrow(
      'last state: connecting, network state: UdpHandshaking, attempt: 3/3, rejoin attempts: 0, recent states: connecting',
    );

    expect(dependencyMocks.joinVoiceChannel).toHaveBeenCalledTimes(3);
    expect(dependencyMocks.entersState.mock.calls.map(call => call[2])).toEqual([20_000, 20_000, 20_000]);
    connections.forEach(connection => expect(connection.destroy).toHaveBeenCalledOnce());
    expect(player.voiceConnection).toBeNull();
    expect(console.warn).toHaveBeenCalledTimes(2);
  });

  it('shares a pending join between concurrent requests for the same channel', async () => {
    const ready = makeDeferred<void>();
    const connection = makeInitialConnection();
    dependencyMocks.joinVoiceChannel.mockReturnValue(connection);
    dependencyMocks.entersState.mockReturnValue(ready.promise);
    const player = new Player({} as never, GUILD_ID);
    const first = player.connect(channel as never);
    await flushAsyncWork();
    const second = player.connect(channel as never);
    ready.resolve(undefined);
    await Promise.all([first, second]);

    expect(dependencyMocks.joinVoiceChannel).toHaveBeenCalledOnce();
    expect(connection.destroy).not.toHaveBeenCalled();
  });

  it('does not retry or reconnect after an explicit disconnect during the Ready wait', async () => {
    const ready = makeDeferred<void>();
    const connection = makeInitialConnection();
    dependencyMocks.joinVoiceChannel.mockReturnValue(connection);
    dependencyMocks.entersState.mockReturnValue(ready.promise);
    const player = new Player({} as never, GUILD_ID);
    const connecting = player.connect(channel as never);
    await flushAsyncWork();
    player.disconnect();
    ready.reject(new Error('destroyed'));
    await expect(connecting).rejects.toThrow('Failed to connect');

    expect(dependencyMocks.joinVoiceChannel).toHaveBeenCalledOnce();
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(player.voiceConnection).toBeNull();
  });

  it('cancels a pending settings lookup before it can join or replace a newer connection', async () => {
    const settings = makeDeferred<object>();
    dependencyMocks.getGuildSettings.mockReturnValueOnce(settings.promise);
    const connection = makeInitialConnection();
    dependencyMocks.joinVoiceChannel.mockReturnValue(connection);
    const player = new Player({} as never, GUILD_ID);
    const first = player.connect(channel as never);
    await player.connect({...channel, id: 'new-channel'} as never);
    settings.resolve({defaultVolume: 100});
    await expect(first).rejects.toThrow('cancelled');

    expect(dependencyMocks.joinVoiceChannel).toHaveBeenCalledOnce();
    expect(player.voiceConnection).toBe(connection);
    expect(connection.destroy).not.toHaveBeenCalled();
  });

  it('waits through an initial connection replacement instead of waiting on its destroyed predecessor', async () => {
    const ready = makeDeferred<void>();
    const first = makeInitialConnection();
    const second = makeInitialConnection();
    dependencyMocks.joinVoiceChannel.mockReturnValueOnce(first).mockReturnValueOnce(second);
    dependencyMocks.entersState.mockReturnValueOnce(ready.promise).mockResolvedValue(undefined);
    const player = new Player({} as never, GUILD_ID);
    const connecting = player.connect(channel as never);
    await flushAsyncWork();
    const waiting = player.ensureVoiceConnectionReady();
    ready.reject(new Error('timeout'));
    await connecting;

    await expect(waiting).resolves.toBe(second);
    expect(dependencyMocks.entersState.mock.calls.map(call => call[0])).toEqual([first, second, second]);
  });

  it('rejects a stale readiness wait without returning an unchecked replacement', async () => {
    const ready = makeDeferred<void>();
    dependencyMocks.entersState.mockReturnValue(ready.promise);
    const oldConnection = makeInitialConnection();
    const newConnection = makeInitialConnection();
    const player = new Player({} as never, GUILD_ID);
    player.voiceConnection = oldConnection as never;
    const waiting = player.ensureVoiceConnectionReady();
    player.voiceConnection = newConnection as never;
    ready.resolve(undefined);

    await expect(waiting).rejects.toThrow('Voice connection changed');
    expect(newConnection.destroy).not.toHaveBeenCalled();
  });

});

describe('bounded media cache identity', () => {
  it('does not reuse a cached first chapter as the complete recording', async () => {
    dependencyMocks.execa.mockResolvedValue({stdout: JSON.stringify({url: 'https://media.example/audio.webm', is_live: false})});
    const entries = new Map<string, {path: string; generation: string}>();
    const fileCache = {getEntryFor: vi.fn(async (key: string) => entries.get(key) ?? null)};
    const player = new Player(fileCache as never, GUILD_ID);
    const createReadStream = vi.fn(async () => new PassThrough());
    Object.assign(player, {createReadStream});
    const song: QueuedSong = {title: 'First chapter', artist: 'Artist', url: 'abcdefghijk', length: 30,
      offset: 0, playlist: null, isLive: false, thumbnailUrl: null, source: MediaSource.Youtube,
      addedInChannelId: 'text-id', requestedBy: 'user-id'};
    const getStream = (song: QueuedSong, options: {seek: number; to: number}) => getOwnedStream(player, song, options);
    (await getStream(song, {seek: 0, to: 30})).destroy();
    entries.set(fileCache.getEntryFor.mock.calls[0][0], {path: '/cache/first-chapter.webm', generation: 'chapter'});
    (await getStream({...song, title: 'Complete recording', length: 180}, {seek: 0, to: 180})).destroy();
    expect(createReadStream).toHaveBeenLastCalledWith(expect.objectContaining({url: 'https://media.example/audio.webm'}));
    expect(dependencyMocks.execa).toHaveBeenCalledTimes(2);
    entries.set(fileCache.getEntryFor.mock.calls[1][0], {path: '/cache/complete.webm', generation: 'full'});
    (await getStream({...song, length: 180}, {seek: 60, to: 180})).destroy();
    expect(createReadStream).toHaveBeenLastCalledWith(expect.objectContaining({url: '/cache/complete.webm', ffmpegInputOptions: ['-ss', '60', '-to', '180']}));
  });
});


describe('FFmpeg termination diagnostics', () => {
  const makeSong = (url = 'abcdefghijk'): QueuedSong => ({title: 'Live', artist: 'Artist', url, length: 3600, offset: 0, playlist: null, isLive: true, thumbnailUrl: null, source: MediaSource.Youtube, addedInChannelId: 'text', requestedBy: 'requester'});

  it('attributes an old normal end to its original queue entry and keeps the reason unknown', async () => {
    const log = vi.spyOn(console, 'info').mockImplementation(() => {});
    dependencyMocks.ffmpeg.mockImplementation(() => makeFfmpegCommand({deferProcessResult: true}));
    const player = new Player({getEntryFor: vi.fn().mockResolvedValue(null)} as never, GUILD_ID);
    const song = makeSong();
    player.add(song); player.add(makeSong('lmnopqrstuv'));
    const entry = player.getCurrentQueueEntryId();
    const stream = await getOwnedStream(player, song);
    player.manualForward(1);
    dependencyMocks.ffmpeg.mock.results[0].value.triggerEnd();
    expect(log).toHaveBeenCalledWith('Playback diagnostic', expect.objectContaining({event: 'ffmpeg-end', queueEntryId: entry, mediaId: 'abcdefghijk', reason: 'unknown'}));
    stream.destroy();
  });

  it('records startup failure but suppresses synchronous errors from an intentional reader kill', async () => {
    const log = vi.spyOn(console, 'info').mockImplementation(() => {});
    const player = new Player({getEntryFor: vi.fn().mockResolvedValue(null)} as never, GUILD_ID);
    dependencyMocks.ffmpeg.mockImplementationOnce(() => makeFfmpegCommand({startupError: new Error('HTTP 403 https://media.example/?token=secret')}));
    await expect(getOwnedStream(player, makeSong())).rejects.toBeInstanceOf(FfmpegStartupError);
    expect(log).toHaveBeenCalledWith('Playback diagnostic', expect.objectContaining({event: 'ffmpeg-error'}));
    expect(JSON.stringify(log.mock.calls)).not.toContain('token=secret');
    log.mockClear();
    dependencyMocks.ffmpeg.mockImplementationOnce(() => makeFfmpegCommand({deferProcessResult: true}));
    const stream = await getOwnedStream(player, makeSong());
    const command = dependencyMocks.ffmpeg.mock.results.at(-1)!.value;
    command.kill.mockImplementation(() => command.triggerError(new Error('intentional SIGKILL')));
    stream.destroy();
    await flushAsyncWork();
    expect(log).not.toHaveBeenCalledWith('Playback diagnostic', expect.objectContaining({event: 'ffmpeg-error'}));
  });

  it('records voice recovery exhaustion for the owned connection', async () => {
    const log = vi.spyOn(console, 'info').mockImplementation(() => {});
    dependencyMocks.entersState.mockRejectedValue(new Error('recovery timed out'));
    const {connection} = makeVoiceConnection({state: {status: 'disconnected', reason: 'websocket-close', closeCode: 4014}});
    const player = new Player({} as never, GUILD_ID);
    player.voiceConnection = connection as never;
    await getPrivatePlayer(player).onVoiceConnectionDisconnect(connection);
    expect(log).toHaveBeenCalledWith('Playback diagnostic', expect.objectContaining({event: 'disconnect', reason: 'voice-recovery-exhausted'}));
  });
});


describe('live ranges, cache isolation and extraction ownership', () => {
  const makeSong = (isLive = false): QueuedSong => ({title: 'Media', artist: 'Artist', url: 'abcdefghijk', length: 1200, offset: 0, playlist: null, isLive, thumbnailUrl: null, source: MediaSource.Youtube, addedInChannelId: 'text', requestedBy: 'user'});

  it.each([true, false])('never applies finite ranges or writes cache when live is known=%s', async knownLive => {
    const cache = {getEntryFor: vi.fn().mockResolvedValue(null), createWriteStream: vi.fn()};
    const player = new Player(cache as never, GUILD_ID);
    const createReadStream = vi.fn().mockResolvedValue(new PassThrough());
    Object.assign(player, {createReadStream});
    const song = makeSong(knownLive);
    (await getOwnedStream(player, song, {seek: 20, to: 1200})).destroy();
    const options = createReadStream.mock.calls[0][0];
    expect(options.ffmpegInputOptions).not.toContain('-ss');
    expect(options.ffmpegInputOptions).not.toContain('-to');
    expect(options.cache).toBe(false);
    expect(song.isLive).toBe(true);
    if (knownLive) expect(cache.getEntryFor).not.toHaveBeenCalled();
  });

  it('does not reuse a legacy cache entry for known live media', async () => {
    const cache = {getEntryFor: vi.fn().mockResolvedValue({path: '/cache/old.webm', generation: 'old'})};
    const player = new Player(cache as never, GUILD_ID);
    const createReadStream = vi.fn().mockResolvedValue(new PassThrough());
    Object.assign(player, {createReadStream});
    (await getOwnedStream(player, makeSong(true))).destroy();
    expect(cache.getEntryFor).not.toHaveBeenCalled();
    expect(createReadStream).toHaveBeenCalledWith(expect.objectContaining({url: 'https://media.example/requested.webm', cache: false}));
  });

  it.each(['disconnect','next-entry'])('rejects stale extraction without FFmpeg or metadata updates after %s', async action => {
    const extraction = makeDeferred<{stdout: string}>();
    dependencyMocks.execa.mockImplementationOnce(() => extraction.promise);
    const player = new Player({getEntryFor: vi.fn().mockResolvedValue(null)} as never, GUILD_ID);
    const song = makeSong();
    const pending = getOwnedStream(player, song, {seek: 0, to: 1200});
    const rejected = expect(pending).rejects.toThrow('Playback attempt was cancelled.');
    await flushAsyncWork();
    if (action === 'disconnect') player.disconnect();
    else {player.add({...makeSong(), url: 'lmnopqrstuv'}); player.manualForward(1);}
    extraction.resolve({stdout: VALID_MEDIA_RESPONSE});
    await rejected;
    expect(dependencyMocks.ffmpeg).not.toHaveBeenCalled();
    expect(song.isLive).toBe(false);
  });

  it('destroys the just-created stream if ownership expires during FFmpeg startup', async () => {
    const startup = makeDeferred<Readable>();
    const player = new Player({getEntryFor: vi.fn().mockResolvedValue(null)} as never, GUILD_ID);
    Object.assign(player, {createReadStream: vi.fn(() => startup.promise)});
    const pending = getOwnedStream(player, makeSong());
    const rejected = expect(pending).rejects.toThrow('Playback attempt was cancelled.');
    await flushAsyncWork();
    player.disconnect();
    const stream = new PassThrough();
    startup.resolve(stream);
    await rejected;
    expect(stream.destroyed).toBe(true);
  });
});


describe('late failed stream work is cancelled', () => {
  it.each(['cache','extraction'])('normalizes a stale %s rejection to cancellation', async stage => {
    const backend = makeDeferred<any>();
    const cache = {getEntryFor: vi.fn().mockImplementation(() => stage === 'cache' ? backend.promise : Promise.resolve(null))};
    if (stage === 'extraction') dependencyMocks.execa.mockImplementationOnce(() => backend.promise);
    const player = new Player(cache as never, GUILD_ID);
    const song: QueuedSong = {title: 'A', artist: 'Artist', url: 'abcdefghijk', length: 1200, offset: 0, playlist: null, isLive: false, thumbnailUrl: null, source: MediaSource.Youtube, addedInChannelId: 'text', requestedBy: 'user'};
    const pending = getOwnedStream(player, song);
    const rejected = expect(pending).rejects.toThrow('Playback attempt was cancelled.');
    await flushAsyncWork();
    player.disconnect();
    backend.reject(new Error('late backend failure'));
    await rejected;
    expect(dependencyMocks.ffmpeg).not.toHaveBeenCalled();
  });
});


describe('bounded YouTube extraction fallbacks', () => {
  it('uses the Android client without the Node-runtime option for an older yt-dlp', async () => {
    process.env.YT_DLP_PATH = '/fake/yt-dlp';
    dependencyMocks.execa
      .mockRejectedValueOnce({stderr: 'yt-dlp: error: no such option: --js-runtimes'})
      .mockResolvedValueOnce({stdout: VALID_MEDIA_RESPONSE});

    await expect(getYouTubeMediaSource('abcdefghijk')).resolves.toMatchObject({
      url: 'https://media.example/requested.webm',
      isLive: true,
    });

    expect(dependencyMocks.execa).toHaveBeenCalledTimes(2);
    const firstArgs = dependencyMocks.execa.mock.calls[0][1] as string[];
    const fallbackArgs = dependencyMocks.execa.mock.calls[1][1] as string[];
    expect(firstArgs).toContain('--js-runtimes');
    expect(fallbackArgs).not.toContain('--js-runtimes');
    expect(fallbackArgs).toContain('--extractor-args');
    expect(fallbackArgs).toContain('youtube:player_client=android');
    expect(fallbackArgs.at(-1)).toBe('https://www.youtube.com/watch?v=abcdefghijk');
  });

  it('does not retry unrelated yt-dlp extraction failures', async () => {
    dependencyMocks.execa.mockRejectedValue({stderr: 'ERROR: upstream TLS handshake failed'});

    await expect(getYouTubeMediaSource('abcdefghijk'))
      .rejects.toThrow('yt-dlp failed to extract media: ERROR: upstream TLS handshake failed');
    expect(dependencyMocks.execa).toHaveBeenCalledOnce();
  });

  it('retries a transient YouTube page reload failure with a fresh extraction process', async () => {
    dependencyMocks.execa
      .mockRejectedValueOnce({stderr: 'ERROR: [youtube] abcdefghijk: The page needs to be reloaded.'})
      .mockResolvedValueOnce({stdout: VALID_MEDIA_RESPONSE});

    await expect(getYouTubeMediaSource('abcdefghijk')).resolves.toMatchObject({
      url: 'https://media.example/requested.webm',
    });

    expect(dependencyMocks.execa).toHaveBeenCalledTimes(2);
    const firstArgs = dependencyMocks.execa.mock.calls[0][1] as string[];
    const fallbackArgs = dependencyMocks.execa.mock.calls[1][1] as string[];
    expect(firstArgs).not.toContain('--extractor-args');
    expect(fallbackArgs).toContain('--extractor-args');
    expect(fallbackArgs).toContain('youtube:player_client=android');
    expect(fallbackArgs.at(-1)).toBe(firstArgs.at(-1));
  });

  it('bounds repeated transient YouTube page reload failures', async () => {
    dependencyMocks.execa.mockRejectedValue({
      stderr: 'ERROR: [youtube] abcdefghijk: The page needs to be reloaded.',
    });

    await expect(getYouTubeMediaSource('abcdefghijk'))
      .rejects.toThrow(
        'yt-dlp failed to extract media: ERROR: [youtube] abcdefghijk: The page needs to be reloaded.',
      );
    expect(dependencyMocks.execa).toHaveBeenCalledTimes(3);
  });


  it('waits for the prior extractor to exit before retrying and preserves extraction flags', async () => {
    const first = makeDeferred<{stdout: string}>();
    dependencyMocks.execa.mockImplementationOnce(() => first.promise);
    const pending = getYouTubeMediaSource('abcdefghijk');
    await flushAsyncWork();
    expect(dependencyMocks.execa).toHaveBeenCalledOnce();
    first.reject({stderr: 'The page needs to be reloaded'});
    await pending;
    expect(dependencyMocks.execa).toHaveBeenCalledTimes(2);
    for (const call of dependencyMocks.execa.mock.calls) {
      expect(call[1]).toEqual(expect.arrayContaining(['-f', 'bestaudio/best', '-S', 'proto:https', '--no-cache-dir']));
      expect(call[2]).toEqual({timeout: 45_000});
    }
  });

  it.each([false,true])('cleans the private cookie copy on fallback success/exhaustion (exhaust=%s)', async exhaust => {
    const directory = await fs.mkdtemp(path.join(tmpdir(), 'muse-fallback-cookies-'));
    const mounted = path.join(directory, 'source.txt');
    await fs.writeFile(mounted, 'fixture-cookies');
    process.env.YT_DLP_COOKIES_PATH = mounted;
    let privatePath = '';
    let attempts = 0;
    dependencyMocks.execa.mockImplementation(async (_bin: string, args: string[]) => {
      attempts++;
      if (attempts === 1) {
        privatePath = args[args.indexOf('--cookies') + 1];
        expect(privatePath).not.toBe(mounted);
        expect((await fs.stat(privatePath)).mode & 0o777).toBe(0o600);
      } else {
        expect(args).toContain('youtube:player_client=android');
        expect(args).not.toContain('--cookies');
      }
      if (attempts === 1 || exhaust) throw {stderr: 'The page needs to be reloaded'};
      return {stdout: VALID_MEDIA_RESPONSE};
    });
    try {
      if (exhaust) await expect(getYouTubeMediaSource('abcdefghijk')).rejects.toThrow('The page needs to be reloaded');
      else await getYouTubeMediaSource('abcdefghijk');
      expect(attempts).toBe(exhaust ? 3 : 2);
      await expect(fs.access(privatePath)).rejects.toMatchObject({code: 'ENOENT'});
      expect(await fs.readFile(mounted, 'utf8')).toBe('fixture-cookies');
    } finally {await fs.rm(directory, {recursive: true, force: true});}
  });

  it('does not read configured cookies for an explicit Android extraction', async () => {
    process.env.YT_DLP_COOKIES_PATH = '/missing/never-read-cookie-source';
    await expect(getYouTubeMediaSource('abcdefghijk', {playerClient: 'android'})).resolves.toHaveProperty('url');
    expect(dependencyMocks.execa.mock.calls[0][1]).toContain('youtube:player_client=android');
    expect(dependencyMocks.execa.mock.calls[0][1]).not.toContain('--cookies');
  });
});


describe('YouTube FFmpeg startup HTTP 403 fallback', () => {
  const song = (source = MediaSource.Youtube): QueuedSong => ({title: 'Startup media', artist: 'Artist', url: source === MediaSource.Youtube ? 'abcdefghijk' : 'https://soundcloud.com/artist/track', length: 3600, offset: 0, playlist: null, isLive: false, thumbnailUrl: null, source, addedInChannelId: 'text', requestedBy: 'user'});
  const forbidden = () => makeFfmpegCommand({startupError: new Error('Server returned 403 Forbidden')});

  it('classifies the original startup error and re-extracts once with Android', async () => {
    expect(new FfmpegStartupError(new Error('Server returned 403 Forbidden'))).toHaveProperty('reason', 'http-403');
    expect(new FfmpegStartupError(new Error('spawn ffmpeg ENOENT'))).toHaveProperty('reason', 'other');
    dependencyMocks.ffmpeg.mockImplementationOnce(forbidden).mockImplementationOnce(makeFfmpegCommand);
    const player = new Player({getEntryFor: vi.fn().mockResolvedValue(null)} as never, GUILD_ID);
    const stream = await getOwnedStream(player, song());
    expect(dependencyMocks.ffmpeg).toHaveBeenCalledTimes(2);
    expect(dependencyMocks.execa).toHaveBeenCalledTimes(2);
    expect(dependencyMocks.execa.mock.calls[1][1]).toContain('youtube:player_client=android');
    stream.destroy();
  });

  it('propagates the second failure without a third FFmpeg startup', async () => {
    const log = vi.spyOn(console, 'info').mockImplementation(() => {});
    dependencyMocks.ffmpeg.mockImplementation(forbidden);
    const player = new Player({getEntryFor: vi.fn().mockResolvedValue(null)} as never, GUILD_ID);
    await expect(getOwnedStream(player, song())).rejects.toBeInstanceOf(FfmpegStartupError);
    expect(dependencyMocks.ffmpeg).toHaveBeenCalledTimes(2);
    expect(dependencyMocks.execa).toHaveBeenCalledTimes(2);
    expect(log).toHaveBeenCalledWith('Playback diagnostic', expect.objectContaining({reason: 'http-403-android-fallback', event: 'ffmpeg-error'}));
  });

  it.each([
    [MediaSource.SoundCloud, 'Server returned 403 Forbidden'],
    [MediaSource.Youtube, 'Server returned 404 Not Found'],
    [MediaSource.Youtube, 'Server returned 410 Gone'],
    [MediaSource.Youtube, 'spawn ffmpeg ENOENT'],
  ])('does not retry other providers or failures (%s, %s)', async (source, message) => {
    dependencyMocks.ffmpeg.mockImplementation(() => makeFfmpegCommand({startupError: new Error(message as string)}));
    const player = new Player({getEntryFor: vi.fn().mockResolvedValue(null)} as never, GUILD_ID);
    await expect(getOwnedStream(player, song(source as MediaSource))).rejects.toThrow();
    expect(dependencyMocks.ffmpeg).toHaveBeenCalledOnce();
    expect(dependencyMocks.execa).toHaveBeenCalledOnce();
  });

  it('does not use network fallback for cached-input 403 or after a playable packet', async () => {
    dependencyMocks.ffmpeg.mockImplementationOnce(forbidden);
    const player = new Player({getEntryFor: vi.fn().mockResolvedValue({path: '/cache/vod.webm', generation: 'cache'})} as never, GUILD_ID);
    await expect(getOwnedStream(player, song())).rejects.toBeInstanceOf(FfmpegStartupError);
    expect(dependencyMocks.execa).not.toHaveBeenCalled();
    dependencyMocks.ffmpeg.mockReset().mockImplementation(() => makeFfmpegCommand({deferProcessResult: true}));
    const fresh = new Player({getEntryFor: vi.fn().mockResolvedValue(null)} as never, GUILD_ID);
    const stream = await getOwnedStream(fresh, song());
    dependencyMocks.ffmpeg.mock.results[0].value.triggerError(new Error('Server returned 403 Forbidden'));
    await flushAsyncWork();
    expect(dependencyMocks.ffmpeg).toHaveBeenCalledOnce();
    expect(dependencyMocks.execa).toHaveBeenCalledOnce();
    stream.destroy();
  });

  it.each(['disconnect','next'])('does not re-extract if %s follows the first FFmpeg error', async action => {
    dependencyMocks.ffmpeg.mockImplementationOnce(() => makeFfmpegCommand({hangOutput: true}));
    const player = new Player({getEntryFor: vi.fn().mockResolvedValue(null)} as never, GUILD_ID);
    const pending = getOwnedStream(player, song());
    const rejected = expect(pending).rejects.toThrow('Playback attempt was cancelled.');
    void rejected.catch(() => {});
    await flushAsyncWork();
    dependencyMocks.ffmpeg.mock.results[0].value.triggerError(new Error('Server returned 403 Forbidden'));
    if (action === 'disconnect') player.disconnect();
    else {player.add({...song(), url: 'lmnopqrstuv'}); player.manualForward(1);}
    await rejected;
    expect(dependencyMocks.ffmpeg).toHaveBeenCalledOnce();
    expect(dependencyMocks.execa).toHaveBeenCalledOnce();
  });

  it.each(['disconnect','next'])('does not start a second FFmpeg if %s occurs during Android extraction', async action => {
    const extraction = makeDeferred<{stdout: string}>();
    let calls = 0;
    dependencyMocks.execa.mockImplementation(() => ++calls === 1 ? Promise.resolve({stdout: JSON.stringify({url: 'https://media.example/first.webm', is_live: false})}) : extraction.promise);
    dependencyMocks.ffmpeg.mockImplementationOnce(forbidden);
    const player = new Player({getEntryFor: vi.fn().mockResolvedValue(null)} as never, GUILD_ID);
    const current = song();
    const pending = getOwnedStream(player, current);
    const rejected = expect(pending).rejects.toThrow('Playback attempt was cancelled.');
    void rejected.catch(() => {});
    await flushAsyncWork();
    expect(dependencyMocks.execa).toHaveBeenCalledTimes(2);
    if (action === 'disconnect') player.disconnect();
    else {player.add({...song(), url: 'lmnopqrstuv'}); player.manualForward(1);}
    extraction.resolve({stdout: VALID_MEDIA_RESPONSE});
    await rejected;
    expect(dependencyMocks.ffmpeg).toHaveBeenCalledOnce();
    expect(current.isLive).toBe(false);
  });
});
