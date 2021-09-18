package edu.cuanschutz.ccp.tm_provider.etl.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ConceptCooccurrenceMetricsTest {

	/**
	 * Test values come from Shakespeare example from Wikipedia:
	 * https://en.wikipedia.org/wiki/Normalized_Google_distance
	 */
	@Test
	public void testNormalizedGoogleDistance() {

		long fx = 130000000l;
		long fy = 26000000l;
		long fxy = 20800000l;
		long N = 25270000000000l;

		double ngd = ConceptCooccurrenceMetrics.normalizedGoogleDistance(fx, fy, fxy, N);

		assertEquals(0.1329, ngd, 0.005);
	}

	@Test
	public void testNormalizedGoogleDistance2() {
		long xConceptCount = 10748;
		long yConceptCount = 15458;
		long pairCount = 6899;
		long totalDocCount = 3478657;
		long totalConceptCount = totalDocCount * 1000;

		double pmi = ConceptCooccurrenceMetrics.normalizedGoogleDistance(xConceptCount, yConceptCount,
				pairCount, totalConceptCount);

		assertEquals(0.0636, pmi, 0.0005);

	}

	@Test
	public void testPointwiseMutualInformation() {
		long xConceptCount = 10748;
		long yConceptCount = 15458;
		long pairCount = 6899;
		long totalDocCount = 3478657;

		double pmi = ConceptCooccurrenceMetrics.pointwiseMutualInformation(totalDocCount, xConceptCount, yConceptCount,
				pairCount);

		assertEquals(4.9729, pmi, 0.0005);

	}

	@Test
	public void testNormalizedPointwiseMutualInformation() {
		long xConceptCount = 10748;
		long yConceptCount = 15458;
		long pairCount = 6899;
		long totalDocCount = 3478657;

		double pmi = ConceptCooccurrenceMetrics.normalizedPointwiseMutualInformation(totalDocCount, xConceptCount,
				yConceptCount, pairCount);

		assertEquals(0.7991, pmi, 0.0005);

	}

	@Test
	public void testMutualDependence() {
		long xConceptCount = 10748;
		long yConceptCount = 15458;
		long pairCount = 6899;
		long totalDocCount = 3478657;

		double pmi = ConceptCooccurrenceMetrics.mutualDependence(totalDocCount, xConceptCount, yConceptCount,
				pairCount);

		assertEquals(-1.250, pmi, 0.0005);

	}

	@Test
	public void testNormalizedPointwiseMutualInformationMaxDenom() {
		long xConceptCount = 10748;
		long yConceptCount = 15458;
		long pairCount = 6899;
		long totalDocCount = 3478657;

		double pmiNormMax = ConceptCooccurrenceMetrics.normalizedPointwiseMutualInformationMaxDenom(totalDocCount,
				xConceptCount, yConceptCount, pairCount);

		assertEquals(0.9181, pmiNormMax, 0.0005);

	}

	@Test
	public void testLogFrequencyBiasedMutualDependence() {
		long xConceptCount = 10748;
		long yConceptCount = 15458;
		long pairCount = 6899;
		long totalDocCount = 3478657;

		double lfmd = ConceptCooccurrenceMetrics.logFrequencyBiasedMutualDependence(totalDocCount, xConceptCount,
				yConceptCount, pairCount);

		assertEquals(-7.4731, lfmd, 0.0005);

	}
	
	
	@Test
	public void testPointwiseMutualInformation2() {
		long xConceptCount = 4;
		long yConceptCount = 4;
		long pairCount = 4;
		long totalDocCount = 4;

		double pmi = ConceptCooccurrenceMetrics.pointwiseMutualInformation(totalDocCount, xConceptCount, yConceptCount,
				pairCount);

		assertEquals(0.0, pmi, 0.0005);

	}

	@Test
	public void testNormalizedPointwiseMutualInformation2() {
		long xConceptCount = 4;
		long yConceptCount = 4;
		long pairCount = 4;
		long totalDocCount = 4;


		double pmi = ConceptCooccurrenceMetrics.normalizedPointwiseMutualInformation(totalDocCount, xConceptCount,
				yConceptCount, pairCount);

		assertEquals(0.0, pmi, 0.0005);

	}

	@Test
	public void testMutualDependence2() {
		long xConceptCount = 4;
		long yConceptCount = 4;
		long pairCount = 4;
		long totalDocCount = 4;


		double pmi = ConceptCooccurrenceMetrics.mutualDependence(totalDocCount, xConceptCount, yConceptCount,
				pairCount);

		assertEquals(0.0, pmi, 0.0005);

	}

	@Test
	public void testNormalizedPointwiseMutualInformationMaxDenom2() {
		long xConceptCount = 4;
		long yConceptCount = 4;
		long pairCount = 4;
		long totalDocCount = 4;


		double pmiNormMax = ConceptCooccurrenceMetrics.normalizedPointwiseMutualInformationMaxDenom(totalDocCount,
				xConceptCount, yConceptCount, pairCount);

		assertEquals(0.0, pmiNormMax, 0.0005);

	}

	@Test
	public void testLogFrequencyBiasedMutualDependence2() {
		long xConceptCount = 4;
		long yConceptCount = 4;
		long pairCount = 4;
		long totalDocCount = 4;


		double lfmd = ConceptCooccurrenceMetrics.logFrequencyBiasedMutualDependence(totalDocCount, xConceptCount,
				yConceptCount, pairCount);

		assertEquals(0.0, lfmd, 0.0005);

	}

}
