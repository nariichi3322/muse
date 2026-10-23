export type PlaybackStopReason = 'manual-disconnect' | 'queue-empty-timeout' | 'no-listeners' | 'channel-unavailable' | 'voice-recovery-exhausted';

export interface PlaybackDiagnosticDetails {
  guildId: string;
  queueEntryId?: number;
  mediaId?: string;
  isLive?: boolean;
  positionSeconds?: number;
  resourcePlaybackDurationMs?: number;
  reason?: string;
  audioState?: string;
  voiceState?: string;
  error?: unknown;
}

export const sanitizePlaybackError = (error: unknown): string => {
  const detail = error instanceof Error ? error.message : String(error);
  return detail
    .replace(/https?:\/\/\S+/gi, '[media URL]')
    .replace(/\b(?:authorization|proxy-authorization|cookies?|set-cookie|headers?|voice[_-]?(?:token|credentials)|session[_-]?id)\b[\s\S]*/gi, '[redacted sensitive details]')
    .replace(/(?:^|[\r\n])\s*[a-z][\w-]*:[^\r\n]*/gi, ' [redacted header]')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 500);
};

export const logPlaybackDiagnostic = (
  event: 'ffmpeg-end' | 'ffmpeg-error' | 'audio-idle' | 'disconnect',
  details: PlaybackDiagnosticDetails,
): void => {
  console.info('Playback diagnostic', {
    event,
    guildId: details.guildId,
    queueEntryId: details.queueEntryId,
    mediaId: details.mediaId,
    isLive: details.isLive,
    positionSeconds: details.positionSeconds,
    resourcePlaybackDurationMs: details.resourcePlaybackDurationMs,
    reason: details.reason ?? 'unknown',
    audioState: details.audioState,
    voiceState: details.voiceState,
    ...(details.error === undefined ? {} : {error: sanitizePlaybackError(details.error)}),
  });
};
