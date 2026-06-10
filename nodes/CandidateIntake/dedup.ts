import { NormalizedIdentity, CandidateRow } from './types';
import { identityFrom } from './normalize';

export function isDuplicate(incoming: NormalizedIdentity, candidate: CandidateRow): boolean {
	if (candidate.deleted_at != null) return false;
	const existing = identityFrom({
		name: candidate.name,
		email: candidate.email,
		phone: candidate.phone,
		postal_code: candidate.postal_code,
		city: candidate.city,
	});
	if (incoming.email != null && existing.email != null && incoming.email === existing.email) {
		return true;
	}
	if (incoming.phone != null && existing.phone != null && incoming.phone === existing.phone) {
		return true;
	}
	if (incoming.name != null && existing.name != null && incoming.name === existing.name) {
		// When incoming has a postal_code, it is the discriminator: a matching
		// city must NOT rescue a postal mismatch. Only fall back to city when
		// incoming has no postal_code at all.
		if (incoming.postal_code != null) {
			if (existing.postal_code != null && incoming.postal_code === existing.postal_code) {
				return true;
			}
		} else if (
			incoming.city != null &&
			existing.city != null &&
			incoming.city === existing.city
		) {
			return true;
		}
	}
	return false;
}

export function findDuplicate(
	incoming: NormalizedIdentity,
	candidates: CandidateRow[],
): CandidateRow | null {
	for (const candidate of candidates) {
		if (isDuplicate(incoming, candidate)) return candidate;
	}
	return null;
}
