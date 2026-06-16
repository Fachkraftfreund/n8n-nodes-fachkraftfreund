// ─── resume.ts ───
// Pure helpers for the CandidateIntake resume-upload feature. No HTTP, no n8n
// imports — just type detection, path/URL construction, and the upload
// predicate, so the riskiest decision logic is unit-tested without I/O.

export const RESUME_BUCKET = 'resumes';
export const MAX_RESUME_BYTES = 15 * 1024 * 1024; // ~15 MB node-side sanity cap

export interface ResumeFileType {
	/** File extension WITHOUT a leading dot, e.g. 'pdf'. */
	ext: string;
	contentType: string;
}

// ─── content-type ↔ extension map ───
// Small, sensible map keyed by canonical Content-Type. Covers the resume
// formats we actually see (pdf, doc, docx) plus a few common types so that
// derive-from-extension works for the same set. There is intentionally no hard
// allowlist: an unrecognized-but-specific Content-Type is kept verbatim (ext
// derived from URL, else pdf), and unknown extensions fall back to pdf.
const CONTENT_TYPE_TO_EXT: Record<string, string> = {
	'application/pdf': 'pdf',
	'application/msword': 'doc',
	'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
	'image/png': 'png',
	'image/jpeg': 'jpg',
	'text/plain': 'txt',
};

// Reverse map (extension → canonical Content-Type) for deriving the
// Content-Type from a URL extension. 'jpeg' is an accepted alias of 'jpg'.
const EXT_TO_CONTENT_TYPE: Record<string, string> = {
	pdf: 'application/pdf',
	doc: 'application/msword',
	docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
	png: 'image/png',
	jpg: 'image/jpeg',
	jpeg: 'image/jpeg',
	txt: 'text/plain',
};

export function detectResumeType(
	contentType: string | null | undefined,
	sourceUrl: string | null | undefined,
): ResumeFileType {
	const header = normalizeContentType(contentType);
	if (header && CONTENT_TYPE_TO_EXT[header]) {
		return { ext: CONTENT_TYPE_TO_EXT[header], contentType: header };
	}
	// Fall back to the URL extension for the ext.
	const urlExt = extensionFromUrl(sourceUrl);
	const ext = urlExt && EXT_TO_CONTENT_TYPE[urlExt] ? urlExt : 'pdf';
	if (header) {
		// Header was specific but unrecognized: keep it verbatim as the
		// Content-Type, deriving the ext from the URL (else pdf).
		return { ext, contentType: header };
	}
	// Header was non-informative: derive both ext and Content-Type from the
	// URL extension, else fall back to pdf.
	return { ext, contentType: EXT_TO_CONTENT_TYPE[ext] };
}

/** Extract a lowercase extension (no dot) from a URL path, ignoring any query
 *  string or fragment. Returns null when there is no usable extension. */
function extensionFromUrl(sourceUrl: string | null | undefined): string | null {
	if (!sourceUrl) return null;
	const path = sourceUrl.split('#', 1)[0].split('?', 1)[0];
	const lastSlash = path.lastIndexOf('/');
	const filename = lastSlash >= 0 ? path.slice(lastSlash + 1) : path;
	const dot = filename.lastIndexOf('.');
	if (dot < 0 || dot === filename.length - 1) return null;
	return filename.slice(dot + 1).toLowerCase();
}

// ─── storage path / URL ───

/** Deterministic object path within the resumes bucket: `<candidateId>.<ext>`. */
export function resumeObjectPath(candidateId: string, ext: string): string {
	return `${candidateId}.${ext}`;
}

/** Public object URL:
 *  `<host>/storage/v1/object/public/resumes/<candidateId>.<ext>`.
 *  Trailing slashes on `host` are stripped. */
export function resumePublicUrl(host: string, candidateId: string, ext: string): string {
	const base = host.replace(/\/+$/, '');
	return `${base}/storage/v1/object/public/${RESUME_BUCKET}/${resumeObjectPath(candidateId, ext)}`;
}

// ─── predicates ───

/** enrich-never-overwrite upload predicate: true iff `resumeSourceUrl` is
 *  non-empty (trimmed) AND `existingResumeUrl` is currently NULL/empty.
 *  `existingResumeUrl` is `unknown` because it comes off a DB row. */
export function shouldUploadResume(
	resumeSourceUrl: string | null | undefined,
	existingResumeUrl: unknown,
): boolean {
	const hasSource = typeof resumeSourceUrl === 'string' && resumeSourceUrl.trim() !== '';
	if (!hasSource) return false;
	const existingEmpty =
		existingResumeUrl == null ||
		(typeof existingResumeUrl === 'string' && existingResumeUrl.trim() === '');
	return existingEmpty;
}

/** true iff `sizeBytes` strictly exceeds `MAX_RESUME_BYTES`. */
export function isResumeTooLarge(sizeBytes: number): boolean {
	return sizeBytes > MAX_RESUME_BYTES;
}

/** Lowercase, trim, and strip any ';' parameters from a Content-Type. Treats
 *  empty/missing and the generic 'application/octet-stream' as non-informative
 *  (returns null) so callers fall through to URL extension / pdf fallback. */
function normalizeContentType(contentType: string | null | undefined): string | null {
	if (!contentType) return null;
	const normalized = contentType.split(';', 1)[0].trim().toLowerCase();
	if (!normalized || normalized === 'application/octet-stream') return null;
	return normalized;
}
