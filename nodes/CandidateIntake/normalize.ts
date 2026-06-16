import { NormalizedIdentity } from './types';

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

/** Title-case a full name, leaving known particles lowercase unless leading. */
function toTitleCaseName(name: string): string {
	return name
		.split(' ')
		.map((word, i) =>
			i > 0 && NAME_PARTICLES.has(word.toLowerCase()) ? word.toLowerCase() : capitalizeWord(word),
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
