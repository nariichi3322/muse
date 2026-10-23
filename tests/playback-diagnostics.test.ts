import {afterEach, describe, expect, it, vi} from 'vitest';
import {logPlaybackDiagnostic} from '../src/utils/playback-diagnostics.js';

afterEach(() => vi.restoreAllMocks());

describe('bounded playback diagnostics', () => {
  it('logs only allowed context, redacts URLs and credentials, and bounds errors', () => {
    const log = vi.spyOn(console, 'info').mockImplementation(() => {});
    logPlaybackDiagnostic('ffmpeg-error', {
      guildId: 'guild', queueEntryId: 7, mediaId: 'abcdefghijk', isLive: true,
      error: new Error('HTTP 403 https://media.example/audio?signature=url-secret\nAuthorization: Bearer header-secret\nCookie: cookie-secret'),
      headers: {Authorization: 'unlisted-secret'}, cookies: 'cookie-secret',
    } as never);
    const output = JSON.stringify(log.mock.calls);
    for (const secret of ['url-secret', 'header-secret', 'cookie-secret', 'unlisted-secret']) expect(output).not.toContain(secret);
    expect(log).toHaveBeenCalledWith('Playback diagnostic', expect.objectContaining({event: 'ffmpeg-error', guildId: 'guild', queueEntryId: 7, mediaId: 'abcdefghijk'}));
    logPlaybackDiagnostic('ffmpeg-error', {guildId: 'guild', error: new Error('x'.repeat(900))});
    expect((log.mock.calls.at(-1)![1] as {error: string}).error.length).toBeLessThanOrEqual(500);
  });

  it('does not infer a live broadcast ended naturally from process end', () => {
    const log = vi.spyOn(console, 'info').mockImplementation(() => {});
    logPlaybackDiagnostic('ffmpeg-end', {guildId: 'guild', isLive: true});
    expect(log).toHaveBeenCalledWith('Playback diagnostic', expect.objectContaining({event: 'ffmpeg-end', reason: 'unknown', isLive: true}));
  });
});
