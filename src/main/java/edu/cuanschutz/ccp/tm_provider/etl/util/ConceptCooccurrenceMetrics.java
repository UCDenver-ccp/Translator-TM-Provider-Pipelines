package edu.cuanschutz.ccp.tm_provider.etl.util;

public class ConceptCooccurrenceMetrics {

	public static double normalizedGoogleDistance(long totalDocCount, long concept1OnlyCount, long concept2OnlyCount,
			long pairCount) {
		double logFx = Math.log10((double) concept1OnlyCount);
		double logFy = Math.log10((double) concept2OnlyCount);
		double logFxy = Math.log10((double) pairCount);
		double logN = Math.log10((double) totalDocCount);

		double ngd = (Math.max(logFx, logFy) - logFxy) / (logN - Math.min(logFx, logFy));
		return ngd;
	}

	public static double pointwiseMutualInformation(long totalDocCount, long concept1OnlyCount, long concept2OnlyCount,
			long pairCount) {
		double pxy = (double) pairCount / (double) totalDocCount;
		double px = (double) concept1OnlyCount / totalDocCount;
		double py = (double) concept2OnlyCount / totalDocCount;

		double pmi = Math.log(pxy / (px * py));

		return pmi;
	}

	public static double normalizedPointwiseMutualInformation(long totalDocCount, long concept1OnlyCount,
			long concept2OnlyCount, long pairCount) {

		double pmi = pointwiseMutualInformation(totalDocCount, concept1OnlyCount, concept2OnlyCount, pairCount);
		double pxy = (double) pairCount / (double) totalDocCount;

		double normalizedPmi = pmi / (-1 * Math.log(pxy));
		return normalizedPmi;
	}

	public static double mutualDependence(long totalDocCount, long concept1OnlyCount, long concept2OnlyCount,
			long pairCount) {
		double pxy = (double) pairCount / (double) totalDocCount;
		double px = (double) concept1OnlyCount / totalDocCount;
		double py = (double) concept2OnlyCount / totalDocCount;

		double md = Math.log(Math.pow(pxy, 2) / (px * py));

		return md;
	}

	public static double normalizedPointwiseMutualInformationMaxDenom(long totalDocCount, long concept1OnlyCount,
			long concept2OnlyCount, long pairCount) {
		double pmi = pointwiseMutualInformation(totalDocCount, concept1OnlyCount, concept2OnlyCount, pairCount);
		double px = (double) concept1OnlyCount / totalDocCount;
		double py = (double) concept2OnlyCount / totalDocCount;

		double normalizedPmiMax = pmi / (-1 * Math.log(Math.max(px, py)));
		return normalizedPmiMax;
	}

	public static double logFrequencyBiasedMutualDependence(long totalDocCount, long concept1OnlyCount,
			long concept2OnlyCount, long pairCount) {
		double md = mutualDependence(totalDocCount, concept1OnlyCount, concept2OnlyCount, pairCount);
		double pxy = (double) pairCount / (double) totalDocCount;

		double lfmd = md + Math.log(pxy);
		return lfmd;
	}

}
