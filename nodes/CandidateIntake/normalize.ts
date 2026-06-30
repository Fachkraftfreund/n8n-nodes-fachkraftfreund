import { CleanedName, NormalizedIdentity } from './types';

export function toE164(phone: string | undefined | null): string | null {
	if (!phone) return null;
	const trimmed = phone.trim();
	const hasPlus = trimmed.startsWith('+');
	let digits = trimmed.replace(/\D/g, '');
	if (!digits) return null;
	if (hasPlus) {
		// Already an international number; keep its country code.
		return `+${digits}`;
	}
	if (digits.startsWith('00')) {
		// 00<cc>... international prefix -> +<cc>...
		return `+${digits.slice(2)}`;
	}
	if (digits.startsWith('0')) {
		// German national number: drop trunk 0, prepend 49.
		return `+49${digits.slice(1)}`;
	}
	// Bare significant digits without trunk 0: assume German.
	return `+49${digits}`;
}

export function normalizeEmailForStorage(email: string | undefined | null): string | null {
	if (!email) return null;
	const normalized = email.trim().toLowerCase();
	return normalized || null;
}

export function normalizePhoneForDedup(phone: string | undefined | null): string | null {
	// Reuse the E.164 canonicalizer so every shape (00.., 0.., +49.., +49..)
	// lands on the same international form, then reduce to the national
	// significant number: drop the German country code and any leading zeros.
	const e164 = toE164(phone);
	if (!e164) return null;
	let digits = e164.slice(1); // drop leading '+'
	if (digits.startsWith('49')) digits = digits.slice(2);
	digits = digits.replace(/^0+/, '');
	return digits || null;
}

const GMAIL_DOMAINS = new Set(['gmail.com', 'googlemail.com']);

export function normalizeEmailForDedup(email: string | undefined | null): string | null {
	const normalized = normalizeEmailForStorage(email);
	if (!normalized) return null;
	const at = normalized.lastIndexOf('@');
	if (at < 0) return normalized;
	const local = normalized.slice(0, at);
	const domain = normalized.slice(at + 1);
	if (!GMAIL_DOMAINS.has(domain)) return normalized;
	// Gmail: strip +tag, then collapse dots in the local part.
	const localNoTag = local.split('+', 1)[0];
	const localNoDots = localNoTag.replace(/\./g, '');
	return `${localNoDots}@${domain}`;
}

function trimToNull(value: string | undefined | null): string | null {
	if (!value) return null;
	const trimmed = value.trim();
	return trimmed || null;
}

export function identityFrom(fields: {
	email?: string | null;
	phone?: string | null;
	name?: string | null;
	postal_code?: string | null;
	city?: string | null;
}): NormalizedIdentity {
	return {
		email: normalizeEmailForDedup(fields.email),
		phone: normalizePhoneForDedup(fields.phone),
		name: normalizeNameForDedup(fields.name),
		postal_code: trimToNull(fields.postal_code),
		city: trimToNull(fields.city),
	};
}

// Lowercase nobiliary/connective particles kept lowercase in German/Dutch/
// Romance names (e.g. "Max von der Berg"). Only honoured when not the leading
// word — a name that starts with the particle still gets capitalized.
const NAME_PARTICLES = new Set([
	'von', 'van', 'de', 'del', 'der', 'den', 'di', 'da', 'le', 'la', 'du',
	'zu', 'zur', 'zum', 'ten', 'ter', 'und',
]);

/** Capitalize a single word, treating hyphen and apostrophe as segment breaks. */
function capitalizeWord(word: string): string {
	return word
		.split('-')
		.map((part) =>
			part
				.split("'")
				.map((seg) => (seg ? seg[0].toUpperCase() + seg.slice(1).toLowerCase() : seg))
				.join("'"),
		)
		.join('-');
}

/**
 * Title-case a full name, leaving known particles lowercase unless they lead.
 * `leading` says whether this string starts the full display name: when false
 * (e.g. a standalone last-name part that follows a first name), even a particle
 * in word 0 stays lowercase, so `name` and `last_name` agree on casing.
 */
function toTitleCaseName(name: string, leading = true): string {
	return name
		.split(' ')
		.map((word, i) =>
			(i > 0 || !leading) && NAME_PARTICLES.has(word.toLowerCase())
				? word.toLowerCase()
				: capitalizeWord(word),
		)
		.join(' ');
}

export function composeName(
	firstname: string | undefined | null,
	lastname: string | undefined | null,
): string {
	const joined = `${firstname ?? ''} ${lastname ?? ''}`.replace(/\s+/g, ' ').trim();
	return toTitleCaseName(joined);
}

/**
 * Repair a letter-spaced name field. A field whose whitespace-split tokens are
 * *all* single characters is treated as letter-spacing and collapsed into
 * words, where a run of **2+ spaces** marks a word boundary
 * (`M A X` → `MAX`, `A N N A  L E N A` → `ANNA LENA`). A field containing any
 * multi-letter token is returned unchanged, so a genuine short name (`Jo`) is
 * never mangled. Casing is left to the title-case step.
 */
export function deSpaceLetters(field: string): string {
	const trimmed = field.trim();
	if (trimmed === '') return field;
	const tokens = trimmed.split(/\s+/);
	if (!tokens.every((t) => t.length === 1)) return field;
	return trimmed
		.split(/\s{2,}/)
		.map((word) => word.replace(/\s+/g, ''))
		.join(' ');
}

/**
 * Lift a leading academic title out of a name field. A leading `Dr`/`Prof`
 * token (any case, optional trailing dot) followed by an actual name is removed
 * and returned normalized to `dr`/`prof`. Only these two are recognized; any
 * other leading token — or a bare `Dr` with no name after it — is left intact.
 */
export function extractAcademicTitle(field: string): { title: string | null; rest: string } {
	const m = field.trim().match(/^(dr|prof)\.?\s+(\S.*)$/i);
	if (!m) return { title: null, rest: field };
	return { title: m[1].toLowerCase(), rest: m[2].trim() };
}

/**
 * Clean one incoming name part (first or last) in isolation: de-space letter-
 * spacing, title-case (particles stay lowercase per `leading`), then lift a
 * leading `Dr`/`Prof` into a normalized title. `leading` is false for a last
 * name so its particles agree with how they read in the composed full name.
 */
export function cleanNamePart(
	field: string | undefined | null,
	options: { leading?: boolean } = {},
): { cleaned: string; title: string | null } {
	const { leading = true } = options;
	const deSpaced = deSpaceLetters(field ?? '');
	const collapsed = deSpaced.replace(/\s+/g, ' ').trim();
	const cased = toTitleCaseName(collapsed, leading);
	const { title, rest } = extractAcademicTitle(cased);
	return { cleaned: rest.trim(), title };
}

/**
 * Clean both intake name parts and compose the candidate name. Each part is
 * cleaned independently (the last name as a non-leading segment so its
 * particles stay lowercase); `name` is the join of the cleaned parts and
 * therefore always equals `firstName + " " + lastName`. The academic title is
 * taken from whichever part carried it, first name winning a tie.
 */
export function cleanName(
	firstname: string | undefined | null,
	lastname: string | undefined | null,
): CleanedName {
	const first = cleanNamePart(firstname, { leading: true });
	const last = cleanNamePart(lastname, { leading: false });
	const name = [first.cleaned, last.cleaned].filter((p) => p !== '').join(' ');
	return {
		name,
		firstName: first.cleaned,
		lastName: last.cleaned,
		title: first.title ?? last.title,
	};
}

/**
 * Decide a candidate's gender by majority vote over known `male`/`female`
 * values. Anything that is not exactly `male`/`female` (e.g. `unknown`) is
 * ignored. A tie or the absence of any `male`/`female` vote yields null.
 */
export function genderMajority(genders: string[]): 'male' | 'female' | null {
	let male = 0;
	let female = 0;
	for (const g of genders) {
		if (g === 'male') male++;
		else if (g === 'female') female++;
	}
	if (male > female) return 'male';
	if (female > male) return 'female';
	return null;
}

export function normalizeNameForDedup(name: string | undefined | null): string | null {
	if (!name) return null;
	const normalized = name
		.replace(/ß/g, 'ss')
		.normalize('NFD')
		.replace(/[̀-ͯ]/g, '') // strip combining accents
		.toLowerCase()
		.replace(/\s+/g, ' ')
		.trim();
	return normalized || null;
}
