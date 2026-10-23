import {VoiceChannel} from 'discord.js';
import {Readable} from 'stream';
import hasha from 'hasha';
import {WriteStream} from 'fs-capacitor';
import ffmpeg from 'fluent-ffmpeg';
import prismMedia from 'prism-media';
import shuffle from 'array-shuffle';
import {
  AudioPlayer,
  AudioPlayerState,
  AudioPlayerStatus, AudioResource,
  createAudioPlayer,
  createAudioResource, DiscordGatewayAdapterCreator,
  entersState,
  joinVoiceChannel,
  NetworkingStatusCode,
  StreamType,
  VoiceConnection,
  VoiceConnectionStatus,
} from '@discordjs/voice';
import FileCacheProvider from './file-cache.js';
import {PlaybackAttemptTracker, type PlaybackAttemptContext, type PlaybackAttemptToken} from './playback-attempt.js';
import {
  DEFAULT_VOLUME,
  MediaSource,
  STATUS,
  type AgeRestrictedFallbackResolver,
  type PlayerEvents,
  type QueuedPlaylist,
  type QueuedSong,
  type SongMetadata,
} from './player-types.js';
import {destroyVoiceConnection, recoverVoiceConnection} from './voice-connection-recovery.js';
import debug from '../utils/debug.js';
import {getGuildSettings} from '../utils/get-guild-settings.js';
import {buildPlayingMessageEmbed} from '../utils/build-embed.js';
import {getSoundCloudMediaSource, getYouTubeMediaSource, YtDlpMediaUnavailableError} from '../utils/yt-dlp.js';
import {Setting} from '@prisma/client';
import {logPlaybackDiagnostic, sanitizePlaybackError, type PlaybackDiagnosticDetails, type PlaybackStopReason} from '../utils/playback-diagnostics.js';

export {DEFAULT_VOLUME, MediaSource, STATUS};
export type {AgeRestrictedFallbackResolver, PlayerEvents, QueuedPlaylist, QueuedSong, SongMetadata};

const FFMPEG_STARTUP_TIMEOUT_MS = 20_000;
const VOICE_CONNECTION_ATTEMPTS = 3;
const VOICE_CONNECTION_ATTEMPT_TIMEOUT_MS = 20_000;

const sanitizeFfmpegError = sanitizePlaybackError;

const getMediaIdentifierForLog = (song: QueuedSong) => (
  song.source === MediaSource.Youtube && /^[\w-]{11}$/.test(song.url)
    ? `youtube=${song.url}`
    : `sourceHash=${hasha(song.url).slice(0, 16)}`
);

export class FfmpegMediaUnavailableError extends Error {
  constructor(error: unknown, public readonly reason: 'invalid-output' | 'not-found' | 'unavailable' = 'unavailable') {
    const detail = sanitizeFfmpegError(error);
    super(`FFmpeg could not start audio transcoding${detail ? `: ${detail}` : ''}`);
    this.name = 'FfmpegMediaUnavailableError';
  }
}

export class FfmpegStartupError extends Error {
  readonly reason: 'http-403' | 'other';

  constructor(error: unknown) {
    const rawDetail = error instanceof Error ? error.message : String(error);
    const detail = sanitizeFfmpegError(error);
    super(`FFmpeg could not initialize audio transcoding${detail ? `: ${detail}` : ''}`);
    this.name = 'FfmpegStartupError';
    this.reason = /(?:HTTP error|Server returned)\s+403\b|\b403 Forbidden\b/i.test(rawDetail) ? 'http-403' : 'other';
  }
}

const getFfmpegStartupError = (error: unknown) => {
  const detail = error instanceof Error ? error.message : String(error);
  const isInvalidOutput = /Invalid data found when processing input/i.test(detail);
  const isNotFound = /(?:HTTP error|Server returned) (?:404|410)|404 Not Found|410 Gone/i.test(detail);

  if (isInvalidOutput) {
    return new FfmpegMediaUnavailableError(error, 'invalid-output');
  }

  if (isNotFound) {
    return new FfmpegMediaUnavailableError(error, 'not-found');
  }

  return new FfmpegStartupError(error);
};

type PlayerPlaybackAttemptContext = PlaybackAttemptContext<QueuedSong, VoiceConnection>;

interface PlaybackReadStreamOptions {
  url: string;
  cacheKey: string;
  ffmpegInputOptions?: string[];
  cache?: boolean;
  diagnostics?: PlaybackDiagnosticDetails;
}

export default class {
  public voiceConnection: VoiceConnection | null = null;
  public status = STATUS.PAUSED;
  public guildId: string;
  public loopCurrentSong = false;
  public loopCurrentQueue = false;
  private currentChannel: VoiceChannel | undefined;
  private voiceConnectionGeneration = 0;
  private pendingConnection?: {channelId: string; promise: Promise<void>};
  private queue: QueuedSong[] = [];
  private queuePosition = 0;
  private audioPlayer: AudioPlayer | null = null;
  private audioResource: AudioResource | null = null;
  private volume?: number;
  private defaultVolume: number = DEFAULT_VOLUME;
  private nowPlaying: QueuedSong | null = null;
  private currentQueueEntryVersion = 0;
  private nowPlayingQueueEntryVersion: number | null = null;
  private readonly playbackAttempts: PlaybackAttemptTracker<QueuedSong, VoiceConnection>;
  private pendingPlaybackAttempt?: PlaybackAttemptToken;
  private recoveringPlaybackAttempt?: PlaybackAttemptToken;
  private readonly pendingAudioWaits = new Map<PlaybackAttemptToken, AbortController>();
  private readonly programmaticallyStoppedAudioPlayers = new WeakSet<AudioPlayer>();
  private playPositionInterval: NodeJS.Timeout | undefined;

  private positionInSeconds = 0;
  private readonly fileCache: FileCacheProvider;
  private readonly ageRestrictedFallbackResolver?: AgeRestrictedFallbackResolver;
  private disconnectTimer: NodeJS.Timeout | null = null;

  private readonly channelToSpeakingUsers: Map<string, Set<string>> = new Map();
  private volumeBeforeVoiceActivity?: number;
  private voiceActivityVolumeTarget?: number;
  private voiceActivitySessionGeneration = 0;
  private hasRegisteredVoiceActivityListener = false;

  constructor(fileCache: FileCacheProvider, guildId: string, ageRestrictedFallbackResolver?: AgeRestrictedFallbackResolver) {
    this.fileCache = fileCache;
    this.guildId = guildId;
    this.ageRestrictedFallbackResolver = ageRestrictedFallbackResolver;
    this.playbackAttempts = new PlaybackAttemptTracker(() => ({
      currentSong: this.getCurrent(),
      queueEntryVersion: this.getCurrentQueueEntryId(),
      currentConnection: this.voiceConnection,
    }));
  }

  async connect(channel: VoiceChannel): Promise<void> {
    if (this.pendingConnection?.channelId === channel.id) {
      return this.pendingConnection.promise;
    }

    this.disconnect();
    const promise = this.connectWithRetries(channel, this.voiceConnectionGeneration);
    this.pendingConnection = {channelId: channel.id, promise};

    try {
      await promise;
    } finally {
      if (this.pendingConnection?.promise === promise) {
        this.pendingConnection = undefined;
      }
    }
  }

  async ensureVoiceConnectionReady(): Promise<VoiceConnection> {
    // An initial join may replace a timed-out connection before it succeeds.
    if (this.pendingConnection) {
      await this.pendingConnection.promise;
    }

    const {voiceConnection} = this;
    if (voiceConnection === null) {
      throw new Error('Not connected to a voice channel.');
    }

    await this.waitForVoiceConnectionReady(voiceConnection);
    if (this.voiceConnection !== voiceConnection) {
      throw new Error('Voice connection changed while waiting for it to become ready.');
    }

    return voiceConnection;
  }

  disconnect(reason: PlaybackStopReason = 'manual-disconnect'): void {
    if (this.disconnectTimer) {
      clearTimeout(this.disconnectTimer);
      this.disconnectTimer = null;
    }

    if (this.voiceConnection) {
      logPlaybackDiagnostic('disconnect', {...this.getPlaybackDiagnostics(), reason});
    }

    this.voiceConnectionGeneration++;
    this.pendingConnection = undefined;
    this.invalidatePlaybackAttempts();
    this.pendingPlaybackAttempt = undefined;
    this.voiceActivitySessionGeneration++;

    if (this.voiceConnection) {
      if (this.status === STATUS.PLAYING) {
        this.pause();
      }

      this.loopCurrentSong = false;
      destroyVoiceConnection(this.voiceConnection);
      this.stopAudioPlayer(true);

      this.voiceConnection = null;
      this.audioPlayer = null;
      this.audioResource = null;
      this.currentChannel = undefined;
      this.channelToSpeakingUsers.clear();
      this.volumeBeforeVoiceActivity = undefined;
      this.voiceActivityVolumeTarget = undefined;
      this.hasRegisteredVoiceActivityListener = false;
    }
  }

  async seek(positionSeconds: number): Promise<void> {
    if (this.getCurrent()?.isLive) {
      throw new Error('Seeking is not supported for livestreams.');
    }

    const attempt = this.beginPlaybackAttempt();
    this.pendingPlaybackAttempt = attempt;
    try {
      await this.seekWithAttempt(positionSeconds, attempt);
    } finally {
      if (this.pendingPlaybackAttempt === attempt) {
        this.pendingPlaybackAttempt = undefined;
        this.recoveringPlaybackAttempt = undefined;
      }
    }
  }

  async forwardSeek(positionSeconds: number): Promise<void> {
    return this.seek(this.positionInSeconds + positionSeconds);
  }

  getPosition(): number {
    return this.positionInSeconds;
  }

  async play(allowAgeRestrictedFallback = true): Promise<void> {
    const attempt = this.beginPlaybackAttempt();
    this.pendingPlaybackAttempt = attempt;
    try {
      await this.playWithAttempt(attempt, allowAgeRestrictedFallback);
    } finally {
      if (this.pendingPlaybackAttempt === attempt) {
        this.pendingPlaybackAttempt = undefined;
        this.recoveringPlaybackAttempt = undefined;
      }
    }
  }

  pause(): void {
    if (this.status !== STATUS.PLAYING
      && (!this.pendingPlaybackAttempt || !this.playbackAttempts.isLatest(this.pendingPlaybackAttempt))) {
      throw new Error('Not currently playing.');
    }

    this.invalidatePlaybackAttempts();
    this.status = STATUS.PAUSED;

    if (this.audioPlayer) {
      if (this.audioPlayer.state.status === AudioPlayerStatus.Buffering
        || this.audioPlayer.state.status === AudioPlayerStatus.Idle) {
        // Buffering cannot be paused, and an Idle resource cannot be resumed.
        this.stopAudioPlayer(true);
        this.audioPlayer = null;
        this.audioResource = null;
      } else {
        this.audioPlayer.pause();
      }
    }

    this.stopTrackingPosition();
  }

  async forward(skip: number): Promise<void> {
    const originalPosition = this.positionInSeconds;
    const originalQueuePosition = this.queuePosition;
    const originalQueueEntryVersion = this.currentQueueEntryVersion;
    this.manualForward(skip);
    const destinationSong = this.getCurrent();
    const destinationQueueEntryVersion = this.currentQueueEntryVersion;
    let destinationPlayback: PlayerPlaybackAttemptContext | null = null;

    try {
      if (!destinationSong) {
        await this.finishQueue();
      } else if (this.status !== STATUS.PAUSED
        || this.isPlaybackStarting()) {
        const playPromise = this.play();
        const destinationConnection = this.voiceConnection;
        if (destinationConnection && destinationSong && destinationQueueEntryVersion !== null) {
          destinationPlayback = this.playbackAttempts.capture(
            this.playbackAttempts.latest(),
            destinationSong,
            destinationQueueEntryVersion,
            destinationConnection,
          );
        }

        await playPromise;
      }
    } catch (error: unknown) {
      const failedTransitionStillOwnsDestination = this.getCurrent() === destinationSong
        && this.currentQueueEntryVersion === destinationQueueEntryVersion
        && (destinationPlayback === null || this.playbackAttempts.owns(destinationPlayback));
      if (failedTransitionStillOwnsDestination) {
        this.positionInSeconds = originalPosition;
        this.queuePosition = originalQueuePosition;
        this.currentQueueEntryVersion = originalQueueEntryVersion;
      }

      throw error;
    }
  }

  registerVoiceActivityListener(guildSettings: Setting) {
    const {turnDownVolumeWhenPeopleSpeak, turnDownVolumeWhenPeopleSpeakTarget} = guildSettings;
    const {voiceConnection, currentChannel} = this;
    if (!turnDownVolumeWhenPeopleSpeak || !voiceConnection || !currentChannel) {
      return;
    }

    const voiceActivitySessionGeneration = ++this.voiceActivitySessionGeneration;
    const isCurrentVoiceActivitySession = () => (
      voiceActivitySessionGeneration === this.voiceActivitySessionGeneration
      && voiceConnection === this.voiceConnection
      && currentChannel === this.currentChannel
    );

    voiceConnection.receiver.speaking.on('start', (userId: string) => {
      if (!isCurrentVoiceActivitySession()) {
        return;
      }

      const member = currentChannel.members.get(userId);
      const {id: channelId} = currentChannel;

      if (member) {
        if (!this.channelToSpeakingUsers.has(channelId)) {
          this.channelToSpeakingUsers.set(channelId, new Set());
        }

        this.channelToSpeakingUsers.get(channelId)?.add(member.id);
      }

      this.suppressVoiceWhenPeopleAreSpeaking(turnDownVolumeWhenPeopleSpeakTarget);
    });

    voiceConnection.receiver.speaking.on('end', (userId: string) => {
      if (!isCurrentVoiceActivitySession()) {
        return;
      }

      this.channelToSpeakingUsers.get(currentChannel.id)?.delete(userId);

      this.suppressVoiceWhenPeopleAreSpeaking(turnDownVolumeWhenPeopleSpeakTarget);
    });
  }

  suppressVoiceWhenPeopleAreSpeaking(turnDownVolumeWhenPeopleSpeakTarget: number): void {
    if (!this.currentChannel) {
      return;
    }

    const speakingUsers = this.channelToSpeakingUsers.get(this.currentChannel.id);
    if (speakingUsers && speakingUsers.size > 0) {
      if (this.volumeBeforeVoiceActivity === undefined) {
        this.volumeBeforeVoiceActivity = this.getVolume();
      }

      this.voiceActivityVolumeTarget = turnDownVolumeWhenPeopleSpeakTarget;
      this.setAudioPlayerVolume(turnDownVolumeWhenPeopleSpeakTarget);
    } else if (this.volumeBeforeVoiceActivity !== undefined) {
      const {volumeBeforeVoiceActivity} = this;
      this.volumeBeforeVoiceActivity = undefined;
      this.voiceActivityVolumeTarget = undefined;
      this.setAudioPlayerVolume(volumeBeforeVoiceActivity);
    }
  }

  canGoForward(skip: number) {
    return (this.queuePosition + skip - 1) < this.queue.length;
  }

  manualForward(skip: number): void {
    if (this.canGoForward(skip)) {
      this.queuePosition += skip;
      this.currentQueueEntryVersion++;
      this.positionInSeconds = 0;
      this.stopTrackingPosition();
    } else {
      throw new Error('No songs in queue to forward to.');
    }
  }

  canGoBack() {
    return this.queuePosition - 1 >= 0;
  }

  async back(): Promise<void> {
    if (!this.canGoBack()) {
      throw new Error('No songs in queue to go back to.');
    }

    const originalPosition = this.positionInSeconds;
    const originalQueuePosition = this.queuePosition;
    const originalQueueEntryVersion = this.currentQueueEntryVersion;
    this.queuePosition--;
    this.currentQueueEntryVersion++;
    this.positionInSeconds = 0;
    this.stopTrackingPosition();
    const destinationSong = this.getCurrent();
    const destinationQueueEntryVersion = this.currentQueueEntryVersion;
    let destinationPlayback: PlayerPlaybackAttemptContext | null = null;

    try {
      if (this.status !== STATUS.PAUSED
        || this.isPlaybackStarting()) {
        const playPromise = this.play();
        const destinationConnection = this.voiceConnection;
        if (destinationConnection && destinationSong) {
          destinationPlayback = this.playbackAttempts.capture(
            this.playbackAttempts.latest(),
            destinationSong,
            destinationQueueEntryVersion,
            destinationConnection,
          );
        }

        await playPromise;
      }
    } catch (error: unknown) {
      const failedTransitionStillOwnsDestination = this.getCurrent() === destinationSong
        && this.currentQueueEntryVersion === destinationQueueEntryVersion
        && (destinationPlayback === null || this.playbackAttempts.owns(destinationPlayback));
      if (failedTransitionStillOwnsDestination) {
        this.positionInSeconds = originalPosition;
        this.queuePosition = originalQueuePosition;
        this.currentQueueEntryVersion = originalQueueEntryVersion;
      }

      throw error;
    }
  }

  getCurrent(): QueuedSong | null {
    if (this.queue[this.queuePosition]) {
      return this.queue[this.queuePosition];
    }

    return null;
  }

  getCurrentQueueEntryId(): number | null {
    return this.getCurrent() === null ? null : this.currentQueueEntryVersion;
  }

  /**
   * Returns queue, not including the current song.
   * @returns {QueuedSong[]}
   */
  getQueue(): QueuedSong[] {
    return this.queue.slice(this.queuePosition + 1);
  }

  add(song: QueuedSong, {immediate = false, immediateOffset = 0} = {}): void {
    const currentSong = this.getCurrent();

    if (immediate) {
      // Add as the next song to be played
      const insertAt = this.queuePosition + immediateOffset + 1;
      this.queue = [...this.queue.slice(0, insertAt), song, ...this.queue.slice(insertAt)];
    } else {
      // Add to end of queue
      this.queue.push(song);
    }

    if (this.getCurrent() !== currentSong) {
      this.currentQueueEntryVersion++;
    }
  }

  shuffle(): void {
    const shuffledSongs = shuffle(this.queue.slice(this.queuePosition + 1));

    this.queue = [...this.queue.slice(0, this.queuePosition + 1), ...shuffledSongs];
  }

  clear(): void {
    const newQueue = [];

    // Don't clear curently playing song
    const current = this.getCurrent();

    if (current) {
      newQueue.push(current);
    }

    this.queuePosition = 0;
    this.queue = newQueue;
  }

  removeFromQueue(index: number, amount = 1): void {
    this.queue.splice(this.queuePosition + index, amount);
  }

  removeCurrent(): void {
    this.queue = [...this.queue.slice(0, this.queuePosition), ...this.queue.slice(this.queuePosition + 1)];
    this.currentQueueEntryVersion++;
  }

  queueSize(): number {
    return this.getQueue().length;
  }

  isQueueEmpty(): boolean {
    return this.queueSize() === 0;
  }

  stop(): void {
    this.disconnect();
    this.queuePosition = 0;
    this.queue = [];
    this.currentQueueEntryVersion++;
  }

  move(from: number, to: number): QueuedSong {
    if (from > this.queueSize() || to > this.queueSize()) {
      throw new Error('Move index is outside the range of the queue.');
    }

    this.queue.splice(this.queuePosition + to, 0, this.queue.splice(this.queuePosition + from, 1)[0]);

    return this.queue[this.queuePosition + to];
  }

  setVolume(level: number): void {
    // Level should be a number between 0 and 100 = 0% => 100%
    this.volume = level;

    if (this.volumeBeforeVoiceActivity === undefined) {
      this.setAudioPlayerVolume(level);
    } else {
      this.volumeBeforeVoiceActivity = level;
      this.setAudioPlayerVolume(this.voiceActivityVolumeTarget);
    }
  }

  getVolume(): number {
    // Only use default volume if player volume is not already set (in the event of a reconnect we shouldn't reset)
    return this.voiceActivityVolumeTarget ?? this.volume ?? this.defaultVolume;
  }

  private async connectWithRetries(channel: VoiceChannel, generation: number): Promise<void> {
    const settings = await getGuildSettings(this.guildId);
    if (generation !== this.voiceConnectionGeneration) {
      throw new Error('Voice connection attempt was cancelled.');
    }

    this.defaultVolume = settings.defaultVolume ?? DEFAULT_VOLUME;

    for (let attempt = 1; attempt <= VOICE_CONNECTION_ATTEMPTS; attempt++) {
      const voiceConnection = joinVoiceChannel({
        channelId: channel.id,
        guildId: channel.guild.id,
        selfDeaf: false,
        adapterCreator: channel.guild.voiceAdapterCreator as DiscordGatewayAdapterCreator,
      });

      this.voiceConnection = voiceConnection;
      this.currentChannel = channel;
      this.hasRegisteredVoiceActivityListener = false;

      voiceConnection.on('error', error => {
        console.error(`Voice connection error for guild ${this.guildId}:`, sanitizePlaybackError(error));
      });

      const stateTransitions = [voiceConnection.state.status];
      voiceConnection.on('stateChange', (oldState, newState) => {
        stateTransitions.push(newState.status);
        if (stateTransitions.length > 10) {
          stateTransitions.shift();
        }

        debug(`Voice connection state changed: ${oldState.status} -> ${newState.status}`);

        if (this.voiceConnection === voiceConnection
          && newState.status === VoiceConnectionStatus.Ready
          && !this.hasRegisteredVoiceActivityListener) {
          this.registerVoiceActivityListener(settings);
          this.hasRegisteredVoiceActivityListener = true;
        }
      });

      voiceConnection.on(
        VoiceConnectionStatus.Disconnected,
        this.onVoiceConnectionDisconnect.bind(this, voiceConnection),
      );

      try {
        // Handshakes must run sequentially so only one connection owns the guild.
        // eslint-disable-next-line no-await-in-loop
        await entersState(voiceConnection, VoiceConnectionStatus.Ready, VOICE_CONNECTION_ATTEMPT_TIMEOUT_MS);
        if (generation !== this.voiceConnectionGeneration || this.voiceConnection !== voiceConnection) {
          throw new Error('Voice connection attempt was cancelled.');
        }

        return;
      } catch {
        const {state, rejoinAttempts} = voiceConnection;
        const networking = 'networking' in state ? state.networking : undefined;
        // Only log the enum, never the networking object: it contains voice credentials.
        const networkState = networking ? NetworkingStatusCode[networking.state.code] : 'unavailable';
        const diagnostics = `last state: ${state.status}, network state: ${networkState}, attempt: ${attempt}/${VOICE_CONNECTION_ATTEMPTS}, rejoin attempts: ${rejoinAttempts}, recent states: ${stateTransitions.join(' -> ')}`;
        const shouldRetry = generation === this.voiceConnectionGeneration
          && this.voiceConnection === voiceConnection
          && (state.status === VoiceConnectionStatus.Connecting || state.status === VoiceConnectionStatus.Signalling)
          && attempt < VOICE_CONNECTION_ATTEMPTS;

        destroyVoiceConnection(voiceConnection);
        if (this.voiceConnection === voiceConnection) {
          this.voiceConnection = null;
          this.currentChannel = undefined;
          this.hasRegisteredVoiceActivityListener = false;
        }

        if (!shouldRetry) {
          throw new Error(`Failed to connect to the voice channel (${diagnostics}).`);
        }

        // A fresh join requests new voice-server/session data; rejoin alone can reuse a stalled handshake.
        console.warn(`Retrying voice connection for guild ${this.guildId} (${diagnostics}).`);
      }
    }
  }

  private async seekWithAttempt(positionSeconds: number, attempt: PlaybackAttemptToken): Promise<void> {
    this.status = STATUS.PAUSED;

    const currentSong = this.getCurrent();
    const currentQueueEntryVersion = this.getCurrentQueueEntryId();
    const voiceConnection = await this.ensureVoiceConnectionReady();

    if (!this.playbackAttempts.isCurrent(attempt, voiceConnection)) {
      return;
    }

    if (!currentSong) {
      throw new Error('No song currently playing');
    }

    if (currentQueueEntryVersion === null) {
      return;
    }

    const playback = this.playbackAttempts.capture(
      attempt,
      currentSong,
      currentQueueEntryVersion,
      voiceConnection,
    );
    if (!this.playbackAttempts.owns(playback)) {
      return;
    }

    if (positionSeconds > currentSong.length) {
      throw new Error('Seek position is outside the range of the song.');
    }

    let realPositionSeconds = positionSeconds;
    let to: number | undefined;
    if (currentSong.offset !== undefined) {
      realPositionSeconds += currentSong.offset;
      to = currentSong.length + currentSong.offset;
    }

    const previousPosition = this.positionInSeconds;
    this.stopTrackingPosition();
    let stream: Readable | undefined;
    let audioPlayer: AudioPlayer | undefined;
    try {
      stream = await this.getStream(currentSong, {seek: realPositionSeconds, to}, playback);
      if (!this.playbackAttempts.owns(playback)) {
        this.destroyStaleStream(stream);
        return;
      }

      audioPlayer = this.createPlaybackAudioPlayer(currentSong);
      this.audioPlayer = audioPlayer;
      voiceConnection.subscribe(audioPlayer);
      this.attachListeners();
      this.playAudioPlayerResource(this.createAudioStream(stream));
      if (!await this.confirmAudioPlayerPlaying(audioPlayer, playback, stream)) {
        return;
      }

      this.startTrackingPosition(currentSong.isLive ? 0 : positionSeconds);
      this.status = STATUS.PLAYING;
      this.nowPlaying = currentSong;
      this.nowPlayingQueueEntryVersion = currentQueueEntryVersion;
    } catch (error: unknown) {
      this.disposeStaleAudioPlayer(audioPlayer, stream);
      if (!this.playbackAttempts.owns(playback)) {
        throw error;
      }

      this.stopAudioPlayer(true);
      stream?.destroy();
      this.positionInSeconds = previousPosition;
      this.status = STATUS.PAUSED;
      this.audioPlayer = null;
      this.audioResource = null;
      this.recoveringPlaybackAttempt = attempt;
      await this.handlePlaybackError(error, playback, true);
    }
  }

  private async playWithAttempt(attempt: PlaybackAttemptToken, allowAgeRestrictedFallback: boolean): Promise<void> {
    const currentSong = this.getCurrent();
    const currentQueueEntryVersion = this.getCurrentQueueEntryId();
    const voiceConnection = await this.ensureVoiceConnectionReady();
    if (!this.playbackAttempts.isCurrent(attempt, voiceConnection)) {
      return;
    }

    if (!currentSong) {
      throw new Error('Queue empty.');
    }

    if (currentQueueEntryVersion === null) {
      return;
    }

    const playback = this.playbackAttempts.capture(attempt, currentSong, currentQueueEntryVersion, voiceConnection);
    if (!this.playbackAttempts.owns(playback)) {
      return;
    }

    if (this.disconnectTimer) {
      clearTimeout(this.disconnectTimer);
      this.disconnectTimer = null;
    }

    const resuming = this.status === STATUS.PAUSED
      && currentSong === this.nowPlaying
      && this.currentQueueEntryVersion === this.nowPlayingQueueEntryVersion;
    const resumableAudioPlayer = resuming && this.audioPlayer
      && (this.audioPlayer.state.status === AudioPlayerStatus.Paused
        || this.audioPlayer.state.status === AudioPlayerStatus.Playing)
      ? this.audioPlayer : undefined;
    if (resuming && !resumableAudioPlayer && !currentSong.isLive) {
      return this.seekWithAttempt(this.getPosition(), attempt);
    }

    const previousPosition = this.positionInSeconds;
    this.stopTrackingPosition();
    let stream: Readable | undefined;
    let audioPlayer: AudioPlayer | undefined;
    try {
      if (resumableAudioPlayer) {
        audioPlayer = resumableAudioPlayer;
        audioPlayer.unpause();
        if (!await this.confirmAudioPlayerPlaying(audioPlayer, playback)) {
          return;
        }

        this.status = STATUS.PLAYING;
        this.startTrackingPosition();
        return;
      }

      this.status = STATUS.PAUSED;
      let positionSeconds: number | undefined;
      let to: number | undefined;
      if (!currentSong.isLive && currentSong.offset !== undefined) {
        positionSeconds = currentSong.offset;
        to = currentSong.length + currentSong.offset;
      }

      stream = await this.getStream(currentSong, {seek: positionSeconds, to}, playback);
      if (!this.playbackAttempts.owns(playback)) {
        this.destroyStaleStream(stream);
        return;
      }

      audioPlayer = this.createPlaybackAudioPlayer(currentSong);
      this.audioPlayer = audioPlayer;
      voiceConnection.subscribe(audioPlayer);
      this.attachListeners();
      this.playAudioPlayerResource(this.createAudioStream(stream));
      if (!await this.confirmAudioPlayerPlaying(audioPlayer, playback, stream)) {
        return;
      }

      this.status = STATUS.PLAYING;
      this.nowPlaying = currentSong;
      this.nowPlayingQueueEntryVersion = currentQueueEntryVersion;
      this.startTrackingPosition(0);
    } catch (error: unknown) {
      this.disposeStaleAudioPlayer(audioPlayer, stream);
      if (!this.playbackAttempts.owns(playback)) {
        throw error;
      }

      this.stopAudioPlayer(true);
      stream?.destroy();
      this.positionInSeconds = previousPosition;
      this.status = STATUS.PAUSED;
      this.audioPlayer = null;
      this.audioResource = null;
      this.recoveringPlaybackAttempt = attempt;
      await this.handlePlaybackError(error, playback, allowAgeRestrictedFallback);
    }
  }

  private beginPlaybackAttempt(): PlaybackAttemptToken {
    this.cancelAudioPlayerWaits();
    this.recoveringPlaybackAttempt = undefined;
    return this.playbackAttempts.begin();
  }

  private invalidatePlaybackAttempts(): void {
    this.playbackAttempts.invalidate();
    this.recoveringPlaybackAttempt = undefined;
    this.cancelAudioPlayerWaits();
  }

  private cancelAudioPlayerWaits(): void {
    for (const controller of this.pendingAudioWaits.values()) {
      controller.abort();
    }

    this.pendingAudioWaits.clear();
  }

  private isPlaybackStarting(): boolean {
    return this.pendingPlaybackAttempt !== undefined
      && this.playbackAttempts.isLatest(this.pendingPlaybackAttempt)
      && this.recoveringPlaybackAttempt !== this.pendingPlaybackAttempt;
  }

  private async waitForAudioPlayerPlaying(audioPlayer: AudioPlayer, controller: AbortController): Promise<void> {
    const timeout = setTimeout(() => {
      controller.abort();
    }, 15_000);
    try {
      await entersState(audioPlayer, AudioPlayerStatus.Playing, controller.signal);
    } finally {
      clearTimeout(timeout);
    }
  }

  private async confirmAudioPlayerPlaying(audioPlayer: AudioPlayer, playback: PlayerPlaybackAttemptContext, stream?: Readable): Promise<boolean> {
    this.requirePlaybackOwnership(playback, stream);
    const controller = new AbortController();
    this.pendingAudioWaits.set(playback.attempt, controller);
    try {
      await this.waitForAudioPlayerPlaying(audioPlayer, controller);
    } catch (error: unknown) {
      const cancelled = controller.signal.aborted && error instanceof Error && error.name === 'AbortError';
      if (!cancelled || this.playbackAttempts.owns(playback)) {
        throw error;
      }
    } finally {
      if (this.pendingAudioWaits.get(playback.attempt) === controller) {
        this.pendingAudioWaits.delete(playback.attempt);
      }
    }

    if (!this.playbackAttempts.owns(playback)) {
      this.disposeStaleAudioPlayer(audioPlayer, stream);
      return false;
    }

    if (audioPlayer.state.status !== AudioPlayerStatus.Playing) {
      throw new Error('Audio playback stopped before startup was confirmed.');
    }

    return true;
  }

  private disposeStaleAudioPlayer(audioPlayer?: AudioPlayer, stream?: Readable): void {
    // A newer pause/resume can reuse the same audio player and resource.
    if (audioPlayer && this.audioPlayer !== audioPlayer) {
      this.programmaticallyStoppedAudioPlayers.add(audioPlayer);
      audioPlayer.stop(true);
      if (stream) {
        this.destroyStaleStream(stream);
      }
    }
  }

  private async handlePlaybackError(
    error: unknown,
    playback: PlayerPlaybackAttemptContext,
    allowAgeRestrictedFallback: boolean,
  ): Promise<void> {
    if (!this.playbackAttempts.owns(playback)) {
      throw error;
    }

    const isGone = typeof error === 'object'
      && error !== null
      && 'statusCode' in error
      && error.statusCode === 410;

    if (error instanceof YtDlpMediaUnavailableError
      && error.reason === 'age-restricted'
      && allowAgeRestrictedFallback) {
      const fallbackHandled = await this.tryAgeRestrictedAudioFallback(playback);
      if (fallbackHandled) {
        return;
      }

      if (!this.playbackAttempts.owns(playback)) {
        throw error;
      }
    }

    if (error instanceof YtDlpMediaUnavailableError
      || error instanceof FfmpegMediaUnavailableError
      || isGone) {
      const detail = sanitizePlaybackError(error instanceof Error ? error : 'media returned HTTP 410');
      console.warn(`Skipping unplayable track for guild ${this.guildId} (${getMediaIdentifierForLog(playback.song)}): ${detail}`);
      await this.advancePastUnplayableTrack();
      return;
    }

    throw error;
  }

  private getHashForCache(url: string): string {
    return hasha(url);
  }

  private createPlaybackAudioPlayer(song: QueuedSong): AudioPlayer {
    return createAudioPlayer({behaviors: {maxMissedFrames: song.isLive ? 500 : 50}});
  }

  private requirePlaybackOwnership(playback: PlayerPlaybackAttemptContext, stream?: Readable): void {
    if (!this.playbackAttempts.owns(playback)) {
      if (stream) {
        this.destroyStaleStream(stream);
      }

      throw new Error('Playback attempt was cancelled.');
    }
  }

  private async createOwnedReadStream(options: PlaybackReadStreamOptions, playback: PlayerPlaybackAttemptContext): Promise<Readable> {
    this.requirePlaybackOwnership(playback);
    try {
      const stream = await this.createReadStream(options);
      this.requirePlaybackOwnership(playback, stream);
      return stream;
    } catch (error: unknown) {
      this.requirePlaybackOwnership(playback);
      throw error;
    }
  }

  private async getStream(song: QueuedSong, options: {seek?: number; to?: number}, playback: PlayerPlaybackAttemptContext): Promise<Readable> {
    try {
      this.requirePlaybackOwnership(playback);
      if (this.status === STATUS.PLAYING) {
        this.stopAudioPlayer();
      } else if (this.status === STATUS.PAUSED) {
        this.stopAudioPlayer(true);
      }

      const diagnostics = this.getPlaybackDiagnostics(song);
      if (song.source === MediaSource.HLS) {
        return await this.createOwnedReadStream({url: song.url, cacheKey: song.url, diagnostics}, playback);
      }

      let ffmpegInput: string | null;
      const ffmpegInputOptions: string[] = [];
      let shouldCacheVideo = false;
      // Cached audio begins at zero, with a generation-specific chapter/end boundary.
      const cacheKey = JSON.stringify(['audio-v2', song.source, song.url, options.to ?? null]);
      const cacheHash = this.getHashForCache(cacheKey);
      const cachedEntry = song.isLive ? undefined : await this.fileCache.getEntryFor(cacheHash);
      this.requirePlaybackOwnership(playback);
      ffmpegInput = cachedEntry?.path ?? null;

      if (!ffmpegInput) {
        this.requirePlaybackOwnership(playback);
        const mediaSource = await (song.source === MediaSource.SoundCloud
          ? getSoundCloudMediaSource(song.url)
          : getYouTubeMediaSource(song.url));
        this.requirePlaybackOwnership(playback);
        if (mediaSource.isLive) {
          song.isLive = true;
          diagnostics.isLive = true;
        }

        ffmpegInput = mediaSource.url;
        const MAX_CACHE_LENGTH_SECONDS = 30 * 60;
        shouldCacheVideo = !song.isLive && song.length < MAX_CACHE_LENGTH_SECONDS && !options.seek;
        debug(shouldCacheVideo ? 'Caching video' : 'Not caching video');
        ffmpegInputOptions.push('-reconnect', '1', '-reconnect_streamed', '1', '-reconnect_delay_max', '5');
        ffmpegInputOptions.push(...this.buildFfmpegHeaderOptions(mediaSource.headers));
      }

      if (!song.isLive && options.seek) {
        ffmpegInputOptions.push('-ss', options.seek.toString());
      }

      if (!song.isLive && options.to) {
        ffmpegInputOptions.push('-to', options.to.toString());
      }

      try {
        return await this.createOwnedReadStream({
          url: ffmpegInput, cacheKey, ffmpegInputOptions, cache: shouldCacheVideo, diagnostics,
        }, playback);
      } catch (error: unknown) {
        this.requirePlaybackOwnership(playback);
        if (cachedEntry && error instanceof FfmpegMediaUnavailableError && error.reason === 'invalid-output') {
          await this.fileCache.invalidate(cacheHash, cachedEntry.generation);
          this.requirePlaybackOwnership(playback);
          return this.getStream(song, options, playback);
        }

        if (!cachedEntry && song.source === MediaSource.Youtube
          && error instanceof FfmpegStartupError && error.reason === 'http-403') {
          logPlaybackDiagnostic('ffmpeg-error', {...diagnostics, reason: 'http-403-retry', error});
          this.requirePlaybackOwnership(playback);
          const fallback = await getYouTubeMediaSource(song.url, {playerClient: 'android'});
          this.requirePlaybackOwnership(playback);
          if (fallback.isLive) {
            song.isLive = true;
          }

          const fallbackInputOptions = [
            '-reconnect',
            '1',
            '-reconnect_streamed',
            '1',
            '-reconnect_delay_max',
            '5',
            ...this.buildFfmpegHeaderOptions(fallback.headers),
          ];
          if (!song.isLive && options.seek) {
            fallbackInputOptions.push('-ss', options.seek.toString());
          }

          if (!song.isLive && options.to) {
            fallbackInputOptions.push('-to', options.to.toString());
          }

          // A second startup failure exits this catch; there is no third launch.
          return await this.createOwnedReadStream({
            url: fallback.url,
            cacheKey,
            ffmpegInputOptions: fallbackInputOptions,
            cache: !song.isLive && song.length < 30 * 60 && !options.seek,
            diagnostics: {...diagnostics, isLive: song.isLive, reason: 'http-403-android-fallback'},
          }, playback);
        }

        throw error;
      }
    } catch (error: unknown) {
      this.requirePlaybackOwnership(playback);
      throw error;
    }
  }

  private startTrackingPosition(initalPosition?: number): void {
    if (initalPosition !== undefined) {
      this.positionInSeconds = initalPosition;
    }

    if (this.playPositionInterval) {
      clearInterval(this.playPositionInterval);
    }

    this.playPositionInterval = setInterval(() => {
      this.positionInSeconds++;
    }, 1000);
  }

  private stopTrackingPosition(): void {
    if (this.playPositionInterval) {
      clearInterval(this.playPositionInterval);
      this.playPositionInterval = undefined;
    }
  }

  private attachListeners(): void {
    if (!this.voiceConnection) {
      return;
    }

    if (!this.audioPlayer) {
      return;
    }

    const {audioPlayer} = this;
    if (audioPlayer.listeners('error').length === 0) {
      audioPlayer.on('error', error => {
        // The voice library transitions failed resources to Idle after emitting
        // this event. Let the existing Idle handler advance the queue once.
        console.error(`Audio player failed for guild ${this.guildId}: ${sanitizeFfmpegError(error)}`);
      });
    }

    const queueEntryVersion = this.currentQueueEntryVersion;
    if (audioPlayer.listeners(AudioPlayerStatus.Idle).length === 0) {
      audioPlayer.on(AudioPlayerStatus.Idle, (oldState, newState) => {
        if (this.programmaticallyStoppedAudioPlayers.has(audioPlayer)
          || this.audioPlayer !== audioPlayer
          || this.currentQueueEntryVersion !== queueEntryVersion) {
          return;
        }

        void this.onAudioPlayerIdle(oldState, newState).catch(error => {
          console.error(`Audio player idle handler failed for guild ${this.guildId}:`, sanitizeFfmpegError(error));
        });
      });
    }
  }

  private async onVoiceConnectionDisconnect(voiceConnection: VoiceConnection): Promise<void> {
    await recoverVoiceConnection(voiceConnection, {
      isCurrent: candidate => this.voiceConnection === candidate,
      dispose: candidate => {
        if (this.voiceConnection === candidate) {
          this.disconnect('voice-recovery-exhausted');
        } else {
          destroyVoiceConnection(candidate);
        }
      },
    });
  }

  private async waitForVoiceConnectionReady(voiceConnection: VoiceConnection): Promise<void> {
    await entersState(voiceConnection, VoiceConnectionStatus.Ready, 60_000);
  }

  private async advancePastUnplayableTrack(): Promise<void> {
    this.manualForward(1);

    if (!this.getCurrent()) {
      await this.finishQueue();
      return;
    }

    await this.play();
  }

  private async tryAgeRestrictedAudioFallback(playback: PlayerPlaybackAttemptContext): Promise<boolean> {
    const {song, queueEntryVersion, attempt, connection} = playback;
    if (!this.ageRestrictedFallbackResolver || song.source !== MediaSource.Youtube) {
      return false;
    }

    if (!this.playbackAttempts.owns(playback)) {
      return true;
    }

    let fallback: SongMetadata | null;
    try {
      fallback = await this.ageRestrictedFallbackResolver(song);
    } catch {
      if (!this.playbackAttempts.owns(playback)) {
        return true;
      }

      console.warn(`Audio fallback search failed for age-restricted track in guild ${this.guildId}.`);
      return false;
    }

    if (!this.playbackAttempts.owns(playback)) {
      return true;
    }

    if (!fallback || fallback.source !== MediaSource.Youtube || fallback.url === song.url) {
      return false;
    }

    const {queuePosition} = this;
    const replacement: QueuedSong = {
      ...fallback,
      playlist: song.playlist,
      addedInChannelId: song.addedInChannelId,
      requestedBy: song.requestedBy,
    };
    this.queue[queuePosition] = replacement;
    const replacementPlayback = this.playbackAttempts.capture(
      attempt,
      replacement,
      queueEntryVersion,
      connection,
    );
    console.warn(`Trying audio fallback for age-restricted YouTube track in guild ${this.guildId}: ${getMediaIdentifierForLog(song)} -> ${getMediaIdentifierForLog(replacement)}`);

    try {
      this.pendingPlaybackAttempt = attempt;
      this.recoveringPlaybackAttempt = undefined;
      await this.playWithAttempt(attempt, false);
      return true;
    } catch (error: unknown) {
      if (this.playbackAttempts.owns(replacementPlayback)
        && this.queue[queuePosition] === replacement) {
        this.queue[queuePosition] = song;
      }

      throw error;
    }
  }

  private async onAudioPlayerIdle(_oldState: AudioPlayerState, newState: AudioPlayerState): Promise<void> {
    if (newState.status === AudioPlayerStatus.Idle && this.status === STATUS.PLAYING) {
      logPlaybackDiagnostic('audio-idle', {...this.getPlaybackDiagnostics(), audioState: newState.status});
    }

    // Automatically advance queued song at end
    if (this.loopCurrentSong && !this.getCurrent()?.isLive
      && newState.status === AudioPlayerStatus.Idle && this.status === STATUS.PLAYING) {
      await this.seek(0);
      return;
    }

    // Automatically re-add current song to queue
    if (this.loopCurrentQueue && !this.getCurrent()?.isLive
      && newState.status === AudioPlayerStatus.Idle && this.status === STATUS.PLAYING) {
      const currentSong = this.getCurrent();

      if (currentSong) {
        this.add(currentSong);
      } else {
        throw new Error('No song currently playing.');
      }
    }

    if (newState.status === AudioPlayerStatus.Idle && this.status === STATUS.PLAYING) {
      if (!this.canGoForward(1)) {
        await this.finishQueue();
        return;
      }

      await this.forward(1);
      const currentSong = this.getCurrent();
      if (!currentSong) {
        return;
      }

      // Auto announce the next song if configured to
      const settings = await getGuildSettings(this.guildId);
      const {autoAnnounceNextSong} = settings;
      if (autoAnnounceNextSong && this.currentChannel) {
        await this.currentChannel.send({
          embeds: [buildPlayingMessageEmbed(this)],
        });
      }
    }
  }

  private async finishQueue(): Promise<void> {
    this.invalidatePlaybackAttempts();
    const attempt = this.playbackAttempts.latest();
    const {voiceConnection} = this;
    const ownsIdle = () => this.playbackAttempts.isLatest(attempt)
      && this.voiceConnection === voiceConnection && this.status === STATUS.IDLE;
    if (this.disconnectTimer) {
      clearTimeout(this.disconnectTimer);
      this.disconnectTimer = null;
    }

    this.stopTrackingPosition();
    this.status = STATUS.IDLE;
    this.stopAudioPlayer(true);
    const settings = await getGuildSettings(this.guildId);
    if (!ownsIdle()) {
      return;
    }

    const {secondsToWaitAfterQueueEmpties} = settings;
    if (secondsToWaitAfterQueueEmpties !== 0) {
      const timer = setTimeout(() => {
        if (this.disconnectTimer !== timer) {
          return;
        }

        this.disconnectTimer = null;
        if (ownsIdle()) {
          this.disconnect('queue-empty-timeout');
        }
      }, secondsToWaitAfterQueueEmpties * 1000);
      this.disconnectTimer = timer;
    }
  }

  private getPlaybackDiagnostics(song = this.getCurrent()): PlaybackDiagnosticDetails {
    return {
      guildId: this.guildId,
      queueEntryId: song === this.getCurrent() ? this.getCurrentQueueEntryId() ?? undefined : undefined,
      mediaId: song ? (song.source === MediaSource.Youtube && /^[\w-]{11}$/.test(song.url) ? song.url : hasha(song.url).slice(0, 16)) : undefined,
      isLive: song?.isLive,
      positionSeconds: this.positionInSeconds,
      resourcePlaybackDurationMs: this.audioResource?.playbackDuration,
      audioState: this.audioPlayer?.state?.status,
      voiceState: this.voiceConnection?.state.status,
    };
  }

  private buildFfmpegHeaderOptions(headers: Record<string, string>) {
    const headerLines = Object.entries(headers)
      .map(([key, value]) => `${key}: ${value}`)
      .join('\r\n');

    if (!headerLines) {
      return [];
    }

    return ['-headers', `${headerLines}\r\n`];
  }

  private async createReadStream(options: PlaybackReadStreamOptions): Promise<Readable> {
    const diagnostics = options.diagnostics ?? this.getPlaybackDiagnostics();
    const logFfmpeg = (event: 'ffmpeg-end' | 'ffmpeg-error', error?: unknown) => {
      const current = diagnostics.queueEntryId !== undefined && diagnostics.queueEntryId === this.getCurrentQueueEntryId();
      logPlaybackDiagnostic(event, {
        ...diagnostics,
        positionSeconds: current ? this.positionInSeconds : diagnostics.positionSeconds,
        resourcePlaybackDurationMs: current ? this.audioResource?.playbackDuration : undefined,
        error,
      });
    };

    return new Promise((resolve, reject) => {
      const capacitor = new WriteStream();
      const startupProbeStream = capacitor.createReadStream();
      const opusProbe = new prismMedia.opus.WebmDemuxer();
      let cacheReadStream: Readable | undefined;
      let cacheWriteStream: ReturnType<FileCacheProvider['createWriteStream']> | undefined;

      if (options?.cache) {
        cacheWriteStream = this.fileCache.createWriteStream(this.getHashForCache(options.cacheKey));
        cacheReadStream = capacitor.createReadStream();
        cacheReadStream.pipe(cacheWriteStream, {end: false});
      }

      const returnedStream = capacitor.createReadStream();
      let cacheWriteAborted = false;
      let cacheReadEnded = false;
      let ffmpegEnded = false;
      let hasReturnedStreamClosed = false;
      let startupSettled = false;
      let startupTimedOut = false;
      let startupTimeout: NodeJS.Timeout | undefined;

      const cleanupStartupListeners = () => {
        if (startupTimeout) {
          clearTimeout(startupTimeout);
          startupTimeout = undefined;
        }

        opusProbe.off('data', onPlayablePacket);
        startupProbeStream.off('end', onEndBeforePlayablePacket);
      };

      const destroyCacheWrite = () => {
        if (cacheWriteAborted) {
          return;
        }

        cacheWriteAborted = true;
        cacheReadStream?.destroy();
        cacheWriteStream?.destroy();
      };

      const finalizeCacheWriteIfComplete = () => {
        if (!cacheWriteAborted
          && cacheReadEnded
          && ffmpegEnded
          && cacheWriteStream
          && !cacheWriteStream.writableEnded) {
          cacheWriteStream.end();
        }
      };

      const rejectStartup = (error: unknown) => {
        if (startupSettled) {
          return;
        }

        startupSettled = true;
        cleanupStartupListeners();
        destroyCacheWrite();
        startupProbeStream.destroy();
        opusProbe.destroy();
        returnedStream.destroy();
        capacitor.destroy();
        reject(error instanceof FfmpegMediaUnavailableError || error instanceof FfmpegStartupError
          ? error
          : new FfmpegStartupError(error));
      };

      const onPlayablePacket = () => {
        if (startupSettled) {
          return;
        }

        startupSettled = true;
        cleanupStartupListeners();
        startupProbeStream.destroy();
        opusProbe.destroy();
        resolve(returnedStream);
      };

      const onEndBeforePlayablePacket = () => {
        if (ffmpegEnded) {
          rejectStartup(new FfmpegMediaUnavailableError(
            new Error('FFmpeg produced no playable Opus audio.'),
            'invalid-output',
          ));
        }
      };

      const onFfmpegEnd = () => {
        logFfmpeg('ffmpeg-end');
        ffmpegEnded = true;
        finalizeCacheWriteIfComplete();
        if (startupProbeStream.readableEnded) {
          onEndBeforePlayablePacket();
        }
      };

      const onBufferError = (error: unknown) => {
        if (!startupSettled) {
          rejectStartup(new FfmpegStartupError(error));
          return;
        }

        console.error(`Playback buffer failed after playback began for guild ${this.guildId}: ${sanitizeFfmpegError(error)}`);
        destroyCacheWrite();
      };

      const onCacheWriteError = (error: unknown) => {
        console.warn(`Disabling cache write for guild ${this.guildId}: ${sanitizeFfmpegError(error)}`);
        destroyCacheWrite();
      };

      const stream = ffmpeg(options.url)
        .inputOptions(options?.ffmpegInputOptions ?? ['-re'])
        .noVideo()
        .audioCodec('libopus')
        .outputFormat('webm')
        .on('error', error => {
          destroyCacheWrite();

          if (hasReturnedStreamClosed) {
            return;
          }

          logFfmpeg('ffmpeg-error', error);
          if (!startupSettled) {
            rejectStartup(getFfmpegStartupError(error));
            return;
          }

          console.error(`FFmpeg stream failed after playback began for guild ${this.guildId}: ${sanitizeFfmpegError(error)}`);
          if (!capacitor.writableEnded) {
            capacitor.end();
          }
        })
        .on('start', () => {
          if (startupTimedOut) {
            stream.kill('SIGKILL');
            return;
          }

          debug('Spawned ffmpeg for playback');
        })
        .on('end', onFfmpegEnd);

      capacitor.on('error', onBufferError);
      startupProbeStream.on('error', onBufferError);
      returnedStream.on('error', onBufferError);
      opusProbe.on('error', onBufferError);
      cacheReadStream?.on('error', onCacheWriteError);
      cacheReadStream?.once('end', () => {
        cacheReadEnded = true;
        finalizeCacheWriteIfComplete();
      });
      cacheWriteStream?.on('error', onCacheWriteError);
      opusProbe.once('data', onPlayablePacket);
      startupProbeStream.once('end', onEndBeforePlayablePacket);
      startupProbeStream.pipe(opusProbe);
      startupTimeout = setTimeout(() => {
        startupTimedOut = true;
        stream.kill('SIGKILL');
        rejectStartup(new FfmpegStartupError(
          new Error(`FFmpeg produced no playable audio within ${FFMPEG_STARTUP_TIMEOUT_MS / 1000} seconds.`),
        ));
      }, FFMPEG_STARTUP_TIMEOUT_MS);

      stream.pipe(capacitor);

      returnedStream.on('close', () => {
        hasReturnedStreamClosed = true;
        if (!options.cache) {
          stream.kill('SIGKILL');
        }
      });
    });
  }

  private createAudioStream(stream: Readable) {
    return createAudioResource(stream, {
      inputType: StreamType.WebmOpus,
      inlineVolume: true,
    });
  }

  private playAudioPlayerResource(resource: AudioResource) {
    if (this.audioPlayer !== null) {
      this.audioResource = resource;
      this.setAudioPlayerVolume();
      this.audioPlayer.play(this.audioResource);
    }
  }

  private setAudioPlayerVolume(level?: number) {
    // Audio resource expects a float between 0 and 1 to represent level percentage
    this.audioResource?.volume?.setVolume((level ?? this.getVolume()) / 100);
  }

  private stopAudioPlayer(force = false): void {
    if (!this.audioPlayer) {
      return;
    }

    this.programmaticallyStoppedAudioPlayers.add(this.audioPlayer);
    this.audioPlayer.stop(force);
  }

  private destroyStaleStream(stream: Readable): void {
    if (!stream.destroyed) {
      stream.destroy();
    }
  }
}
