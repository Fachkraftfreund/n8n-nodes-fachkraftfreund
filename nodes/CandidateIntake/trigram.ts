export function cleanJobTitle(title: string): string {
	return title
		.split(' (m/w/d)')[0]
		.split(' m/w/d')[0]
		.split(' (m/w/x)')[0]
		.split(' m/w/x')[0]
		.replace(/\+\d+/g, '')
		.replace(/\((?![^)]*\/in)[^)]*\)/g, '')
		.replaceAll('"', '')
		.replace(' Teilzeit', '')
		.trim();
}

export function trigrams(s: string): Set<string> {
	const result = new Set<string>();
	const words = s.toLowerCase().match(/[\p{L}\p{N}]+/gu) ?? [];
	for (const word of words) {
		const padded = '  ' + word + ' ';
		for (let i = 0; i + 3 <= padded.length; i++) {
			result.add(padded.slice(i, i + 3));
		}
	}
	return result;
}

export function trigramSimilarity(a: string, b: string): number {
	const ta = trigrams(a);
	const tb = trigrams(b);
	if (ta.size === 0 && tb.size === 0) return 0;
	let intersection = 0;
	for (const t of ta) {
		if (tb.has(t)) intersection++;
	}
	const union = ta.size + tb.size - intersection;
	return union === 0 ? 0 : intersection / union;
}
