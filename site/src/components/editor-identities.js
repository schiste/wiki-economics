export function identityTransition(metadata, wiki, granularity) {
  const transition = metadata?.transitions?.[wiki];
  if (!transition) return null;
  const month = transition.first_full_month;
  const [year, monthNumber] = month.split("-").map(Number);
  const period = granularity === "year"
    ? String(year)
    : granularity === "quarter"
      ? `${year}-Q${Math.ceil(monthNumber / 3)}`
      : month;
  return {...transition, period};
}
