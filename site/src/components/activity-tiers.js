/**
 * Fixed calendar lengths used to translate the monthly activity-rate bands
 * into the selected reporting period.
 */
export function activityTierMonths(granularity) {
  if (granularity === "year") return 12;
  if (granularity === "quarter") return 3;
  return 1;
}

export function activityTierLabels(granularity) {
  const months = activityTierMonths(granularity);
  return [
    months === 1 ? "1 edit" : `1-${months} edits`,
    `${months + 1}-${5 * months - 1} edits`,
    `${5 * months}-${25 * months - 1} edits`,
    `${25 * months}-${100 * months - 1} edits`,
    `${100 * months}+ edits`,
  ];
}
