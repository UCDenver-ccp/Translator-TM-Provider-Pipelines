package edu.cuanschutz.ccp.tm_provider.etl.fn;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

public class AbbreviationFnTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testSerializeAbbreviations() throws IOException {
		String sentence1 = "Aldehyde dehydrogenase 1 (ALDH1) has been shown to protect against Parkinson's disease (PD) by reducing toxic metabolites of dopamine.";
		String sentence2 = "The function of aldehyde dehydrogenase 1A3 (ALDH1A3) in invasion was assessed by performing transwell assays and animal experiments.";

		String documentText = sentence1 + " " + sentence2;

		Map<String, Span> sentenceToSpanMap = new HashMap<String, Span>();
		sentenceToSpanMap.put(sentence1, new Span(0, 134));
		sentenceToSpanMap.put(sentence2, new Span(0 + 135, 132 + 135));

		List<String> results = Arrays.asList(sentence1, "  ALDH1|Aldehyde dehydrogenase 1|0.999613",
				"  PD|Parkinson's disease|0.99818", sentence2, "  ALDH1A3|aldehyde dehydrogenase 1A3|0.999613");

		String serializedAbbreviations = AbbreviationFn.serializeAbbreviations(results, sentenceToSpanMap,
				documentText);

		/*
		 * TODO Note: output order of relations may not be deterministic - so this test
		 * may fail at some point in the future.
		 */
		// @formatter:off
		String expectedAbbreviationsBionlp = 
				"T1\tlong_form 0 24\tAldehyde dehydrogenase 1\n" +
		        "T2\tshort_form 26 31\tALDH1\n" + 
				"T3\tlong_form 67 86\tParkinson's disease\n" +
		        "T4\tshort_form 88 90\tPD\n" +
				"T5\tlong_form 151 177\taldehyde dehydrogenase 1A3\n" + 
		        "T6\tshort_form 179 186\tALDH1A3\n" + 
				"R1\thas_short_form Arg1:T3 Arg2:T4\n" +
				"R2\thas_short_form Arg1:T1 Arg2:T2\n" +
				"R3\thas_short_form Arg1:T5 Arg2:T6\n";
		// @formatter:on

		assertEquals(expectedAbbreviationsBionlp, serializedAbbreviations);
	}

	@Test
	public void testFindNearestShortLongFormSpans() {

		List<Span> shortFormSpans = new ArrayList<Span>(Arrays.asList(new Span(26, 31)));
		List<Span> longFormSpans = new ArrayList<Span>(Arrays.asList(new Span(0, 24)));
		Span[] shortLongFormSpans = AbbreviationFn.findNearestShortLongFormSpans(shortFormSpans, longFormSpans);

		Span[] expectedSpans = new Span[] { new Span(26, 31), new Span(0, 24) };

		assertArrayEquals(expectedSpans, shortLongFormSpans);

		shortFormSpans.add(new Span(55, 60));
		shortLongFormSpans = AbbreviationFn.findNearestShortLongFormSpans(shortFormSpans, longFormSpans);
		assertArrayEquals(expectedSpans, shortLongFormSpans);

		longFormSpans.add(new Span(45, 50));
		shortLongFormSpans = AbbreviationFn.findNearestShortLongFormSpans(shortFormSpans, longFormSpans);
		assertArrayEquals(expectedSpans, shortLongFormSpans);

		// add a pair that is closer together and see if it becomes the pair that is
		// returned
		shortFormSpans.add(new Span(77, 79));
		longFormSpans.add(new Span(66, 76));
		shortLongFormSpans = AbbreviationFn.findNearestShortLongFormSpans(shortFormSpans, longFormSpans);
		assertArrayEquals(new Span[] { new Span(77, 79), new Span(66, 76) }, shortLongFormSpans);

	}

	/**
	 * Make sure that the short form spans don't overlap with the long form, e.g.
	 * Nickel (Ni) 01234567890 Nickel (Ni)
	 */
	@Test
	public void testFindNearestShortLongFormSpans_OverlappingSpans() {
		List<Span> shortFormSpans = Arrays.asList(new Span(0, 2), new Span(8, 10));
		List<Span> longFormSpans = Arrays.asList(new Span(0, 6));
		Span[] shortLongFormSpans = AbbreviationFn.findNearestShortLongFormSpans(shortFormSpans, longFormSpans);
		assertArrayEquals(new Span[] { new Span(8, 10), new Span(0, 6) }, shortLongFormSpans);
	}

	// This data is from PMC9006252
	@Test
	public void testNullPointerExceptionInRealExample() throws IOException {
		File dir = folder.newFolder();
		File ab3pOutputFile = ClassPathUtil.copyClasspathResourceToDirectory(getClass(), "abbrev.sample.ab3p.results",
				dir);

		File sentenceAnnotsInBioNLP = ClassPathUtil.copyClasspathResourceToDirectory(getClass(),
				"abbrev.sample.sentences.bionlp", dir);
		File documentTextFile = ClassPathUtil.copyClasspathResourceToDirectory(getClass(), "abbrev.sample.txt", dir);

		BioNLPDocumentReader reader = new BioNLPDocumentReader();
		TextDocument td = reader.readDocument("123456", "source", sentenceAnnotsInBioNLP, documentTextFile,
				CharacterEncoding.UTF_8);
		Map<String, Span> sentenceToSpanMap = new HashMap<String, Span>();
		for (TextAnnotation annot : td.getAnnotations()) {
			sentenceToSpanMap.put(annot.getCoveredText(), annot.getAggregateSpan());
		}

		List<String> results = FileReaderUtil.loadLinesFromFile(ab3pOutputFile, CharacterEncoding.UTF_8);
		String documentText = IOUtils.toString(new FileInputStream(documentTextFile),
				CharacterEncoding.UTF_8.getCharacterSetName());

		String serializedAbbreviations = AbbreviationFn.serializeAbbreviations(results, sentenceToSpanMap,
				documentText);
	}

	@Test
	public void testDeserializeRealOutput() throws IOException {
		String bionlp = "T1	long_form 663 689	magnetic resonance imaging\n" + "T2	short_form 691 694	MRI\n"
				+ "T3	long_form 756 783	phosphatidylserine receptor\n" + "T4	short_form 785 790	Ptdsr\n"
				+ "T5	long_form 1549 1573	congenital heart disease\n" + "T6	short_form 1575 1578	CHD\n"
				+ "T7	long_form 2566 2587	N-ethyl-N-nitrosourea\n" + "T8	short_form 2589 2592	ENU\n"
				+ "T9	long_form 3020 3036	days post coitum\n" + "T10	short_form 3038 3041	dpc\n"
				+ "T11	long_form 6155 6182	phosphatidylserine receptor\n" + "T12	short_form 6184 6189	Ptdsr\n"
				+ "T13	long_form 8757 8768	spinal cord\n" + "T14	short_form 8770 8772	sc\n"
				+ "T15	long_form 8848 8889	primary atrial and interventricular septa\n"
				+ "T16	short_form 8891 8894	pas\n" + "T17	long_form 8902 8914	mitral valve\n"
				+ "T18	short_form 8916 8918	mv\n" + "T19	long_form 8921 8934	midbrain roof\n"
				+ "T20	short_form 8936 8939	mbr\n" + "T21	long_form 8942 8950	midbrain\n"
				+ "T22	short_form 8952 8954	mb\n" + "T23	short_form 8957 8960	mes\n"
				+ "T24	long_form 8957 8978	mesencephalic vesicle\n" + "T25	short_form 9002 9004	hy\n"
				+ "T26	long_form 9002 9014	hypothalamus\n" + "T27	short_form 9006 9009	tha\n"
				+ "T28	long_form 9006 9014	thalamus\n" + "T29	short_form 9021 9023	po\n"
				+ "T30	long_form 9021 9025	pons\n" + "T31	short_form 9032 9033	c\n"
				+ "T32	long_form 9032 9042	cerebellum\n" + "T33	long_form 9048 9065	medulla oblongata\n"
				+ "T34	short_form 9067 9069	mo\n" + "T35	short_form 9072 9075	pit\n"
				+ "T36	long_form 9072 9081	pituitary\n" + "T37	short_form 9089 9090	t\n"
				+ "T38	long_form 9089 9095	tongue\n" + "T39	short_form 9101 9103	th\n"
				+ "T40	long_form 9101 9107	thymus\n" + "T41	short_form 9169 9171	ao\n"
				+ "T42	long_form 9169 9174	aorta\n" + "T43	short_form 9181 9183	li\n"
				+ "T44	long_form 9181 9186	liver\n" + "T45	short_form 9193 9194	s\n"
				+ "T46	long_form 9193 9200	stomach\n" + "T47	long_form 9206 9229	left adrenal and kidney\n"
				+ "T48	short_form 9231 9234	lad\n" + "T49	short_form 9241 9243	pa\n"
				+ "T50	long_form 9241 9249	pancreas\n" + "T51	long_form 9272 9288	umbilical hernia\n"
				+ "T52	short_form 9290 9292	uh\n" + "T53	long_form 9321 9337	fourth ventricle\n"
				+ "T54	short_form 9339 9341	fv\n" + "T55	short_form 9360 9363	lar\n"
				+ "T56	long_form 9360 9366	larynx\n" + "T57	long_form 9374 9405	right ventricular outflow tract\n"
				+ "T58	short_form 9407 9411	rvot\n" + "T59	short_form 9414 9416	sp\n"
				+ "T60	long_form 9414 9420	spleen\n" + "T61	long_form 9431 9437	testes\n"
				+ "T62	short_form 9434 9436	te\n" + "T63	long_form 9773 9779	Cited2\n"
				+ "T64	short_form 9773 9782	Cited2-/-\n"
				+ "T65	long_form 10575 10605	and ventricular septal defects\n"
				+ "T66	short_form 10607 10610	ASD\n" + "T67	long_form 12451 12471	primary atria septum\n"
				+ "T68	short_form 12473 12476	pas\n" + "T69	long_form 12610 12635	ventricular septal defect\n"
				+ "T70	short_form 12637 12640	VSD\n" + "T71	long_form 12649 12672	interventricular septum\n"
				+ "T72	short_form 12674 12677	ivs\n" + "T73	long_form 12747 12762	ascending aorta\n"
				+ "T74	short_form 12764 12768	a-ao\n" + "T75	long_form 12778 12794	pulmonary artery\n"
				+ "T76	short_form 12796 12798	pa\n" + "T77	long_form 12820 12835	right ventricle\n"
				+ "T78	short_form 12837 12839	rv\n" + "T79	long_form 12846 12858	aortic valve\n"
				+ "T80	short_form 12860 12864	ao-v\n" + "T81	long_form 12918 12929	aortic arch\n"
				+ "T82	short_form 12931 12935	ao-a\n" + "T83	short_form 12965 12967	tr\n"
				+ "T84	long_form 12965 12972	trachea\n" + "T85	short_form 12986 12988	es\n"
				+ "T86	long_form 12986 12995	esophagus\n" + "T87	long_form 13036 13049	aortic arches\n"
				+ "T88	short_form 13051 13055	ao-a\n" + "T89	short_form 13092 13094	tr\n"
				+ "T90	long_form 13092 13099	trachea\n" + "T91	short_form 13113 13115	es\n"
				+ "T92	long_form 13113 13122	esophagus\n" + "T93	short_form 13152 13154	th\n"
				+ "T94	long_form 13152 13158	thymus\n" + "T95	long_form 13172 13196	right superior vena cava\n"
				+ "T96	short_form 13198 13203	r-svc\n" + "T97	long_form 13362 13383	systemic venous sinus\n"
				+ "T98	short_form 13385 13388	svs\n" + "T99	long_form 13391 13414	left superior vena cava\n"
				+ "T100	short_form 13416 13421	l-svc\n" + "T101	long_form 13424 13438	pulmonary vein\n"
				+ "T102	short_form 13440 13443	pvn\n" + "T103	long_form 13446 13462	descending aorta\n"
				+ "T104	short_form 13464 13468	d-ao\n" + "T105	long_form 13513 13536	secondary atrial septum\n"
				+ "T106	short_form 13538 13541	sas\n" + "T107	long_form 13600 13615	pulmonary valve\n"
				+ "T108	short_form 13617 13619	pv\n" + "T109	long_form 13626 13639	arterial duct\n"
				+ "T110	short_form 13641 13643	ad\n" + "T111	long_form 14081 14100	right adrenal gland\n"
				+ "T112	short_form 14102 14105	rad\n" + "T113	long_form 14123 14135	right kidney\n"
				+ "T114	short_form 14137 14139	rk\n" + "T115	long_form 14168 14178	right lung\n"
				+ "T116	short_form 14180 14182	rl\n" + "T117	long_form 14621 14648	phosphatidylserine receptor\n"
				+ "T118	short_form 14650 14658	Ptdsr-/-\n" + "T119	long_form 17836 17857	primary atrial septum\n"
				+ "T120	short_form 17859 17862	pas\n" + "T121	long_form 17939 17965	ventricular septal defects\n"
				+ "T122	short_form 17967 17970	VSD\n" + "T123	short_form 18506 18508	tr\n"
				+ "T124	long_form 18506 18513	trachea\n" + "T125	long_form 18543 18558	ascending aorta\n"
				+ "T126	short_form 18560 18564	a-ao\n"
				+ "T127	long_form 18582 18612	left ventricular outflow tract\n"
				+ "T128	short_form 18614 18618	lvot\n" + "T129	long_form 18629 18641	aortic valve\n"
				+ "T130	short_form 18643 18647	ao-v\n" + "T131	long_form 18674 18685	aortic arch\n"
				+ "T132	short_form 18687 18691	ao-a\n" + "T133	long_form 18710 18726	descending aorta\n"
				+ "T134	short_form 18728 18732	d-ao\n" + "T135	long_form 18739 18755	pulmonary artery\n"
				+ "T136	short_form 18757 18759	pa\n"
				+ "T137	long_form 18777 18808	right ventricular outflow tract\n"
				+ "T138	short_form 18810 18814	rvot\n" + "T139	long_form 18838 18851	arterial duct\n"
				+ "T140	short_form 18853 18855	ad\n" + "T141	long_form 18972 18997	ventricular septal defect\n"
				+ "T142	short_form 18999 19002	VSD\n" + "T143	long_form 19227 19252	ventricular septal defect\n"
				+ "T144	short_form 19254 19257	VSD\n" + "T145	short_form 19421 19423	th\n"
				+ "T146	long_form 19421 19427	thymus\n" + "T147	long_form 19700 19710	regression\n"
				+ "T148	short_form 19703 19704	r\n" + "T149	long_form 19820 19829	wild-type\n"
				+ "T150	short_form 19831 19833	wt\n"
				+ "T151	long_form 19978 20008	were therefore pooled together\n"
				+ "T152	short_form 20010 20014	wt/h\n" + "T153	long_form 20748 20771	interventricular septum\n"
				+ "T154	long_form 20748 20771	interventricular septum\n"
				+ "T155	long_form 20748 20771	interventricular septum\n" + "T156	short_form 20773 20776	ivs\n"
				+ "T157	short_form 20773 20776	ivs\n" + "T158	short_form 20773 20776	ivs\n"
				+ "T159	long_form 20783 20798	ascending aorta\n" + "T160	short_form 20800 20804	a-ao\n"
				+ "T161	long_form 20858 20869	aortic arch\n" + "T162	short_form 20871 20875	ao-a\n"
				+ "T163	long_form 20894 20910	descending aorta\n" + "T164	short_form 20912 20916	d-ao\n"
				+ "T165	long_form 20923 20939	pulmonary artery\n" + "T166	short_form 20941 20943	pa\n"
				+ "T167	long_form 20985 21000	pulmonary valve\n" + "T168	short_form 21002 21004	pv\n"
				+ "T169	long_form 21027 21040	arterial duct\n" + "T170	short_form 21042 21044	ad\n"
				+ "T171	short_form 21116 21118	tr\n" + "T172	long_form 21116 21123	trachea\n"
				+ "T173	long_form 21130 21149	right main bronchus\n" + "T174	short_form 21151 21154	rmb\n"
				+ "T175	short_form 21160 21162	es\n" + "T176	long_form 21160 21169	esophagus\n"
				+ "T177	long_form 21235 21260	ventricular septal defect\n" + "T178	short_form 21262 21265	VSD\n"
				+ "T179	long_form 21545 21557	aortic valve\n" + "T180	short_form 21559 21562	aov\n"
				+ "T181	long_form 29967 30018	gadolinium-diethylenetriamine pentaacetic anhydride\n"
				+ "T182	short_form 30020 30027	Gd-DTPA\n" + "T183	short_form 32237 32238	p\n"
				+ "T184	long_form 32237 32248	probability\n" + "R1	has_short_form Arg1:T56 Arg2:T55\n"
				+ "R2	has_short_form Arg1:T155 Arg2:T158\n" + "R3	has_short_form Arg1:T1 Arg2:T2\n"
				+ "R4	has_short_form Arg1:T159 Arg2:T160\n" + "R5	has_short_form Arg1:T149 Arg2:T150\n"
				+ "R6	has_short_form Arg1:T105 Arg2:T106\n" + "R7	has_short_form Arg1:T77 Arg2:T78\n"
				+ "R8	has_short_form Arg1:T36 Arg2:T35\n" + "R9	has_short_form Arg1:T143 Arg2:T144\n"
				+ "R10	has_short_form Arg1:T40 Arg2:T39\n" + "R11	has_short_form Arg1:T15 Arg2:T16\n"
				+ "R12	has_short_form Arg1:T13 Arg2:T14\n" + "R13	has_short_form Arg1:T86 Arg2:T85\n"
				+ "R14	has_short_form Arg1:T17 Arg2:T18\n" + "R15	has_short_form Arg1:T46 Arg2:T45\n"
				+ "R16	has_short_form Arg1:T119 Arg2:T120\n" + "R17	has_short_form Arg1:T129 Arg2:T130\n"
				+ "R18	has_short_form Arg1:T176 Arg2:T175\n" + "R19	has_short_form Arg1:T163 Arg2:T164\n"
				+ "R20	has_short_form Arg1:T115 Arg2:T116\n" + "R21	has_short_form Arg1:T61 Arg2:T62\n"
				+ "R22	has_short_form Arg1:T107 Arg2:T108\n" + "R23	has_short_form Arg1:T63 Arg2:T64\n"
				+ "R24	has_short_form Arg1:T127 Arg2:T128\n" + "R25	has_short_form Arg1:T131 Arg2:T132\n"
				+ "R26	has_short_form Arg1:T109 Arg2:T110\n" + "R27	has_short_form Arg1:T113 Arg2:T114\n"
				+ "R28	has_short_form Arg1:T181 Arg2:T182\n" + "R29	has_short_form Arg1:T139 Arg2:T140\n"
				+ "R30	has_short_form Arg1:T60 Arg2:T59\n" + "R31	has_short_form Arg1:T19 Arg2:T20\n"
				+ "R32	has_short_form Arg1:T9 Arg2:T10\n" + "R33	has_short_form Arg1:T73 Arg2:T74\n"
				+ "R34	has_short_form Arg1:T11 Arg2:T12\n" + "R35	has_short_form Arg1:T79 Arg2:T80\n"
				+ "R36	has_short_form Arg1:T124 Arg2:T123\n" + "R37	has_short_form Arg1:T67 Arg2:T68\n"
				+ "R38	has_short_form Arg1:T94 Arg2:T93\n" + "R39	has_short_form Arg1:T53 Arg2:T54\n"
				+ "R40	has_short_form Arg1:T38 Arg2:T37\n" + "R41	has_short_form Arg1:T135 Arg2:T136\n"
				+ "R42	has_short_form Arg1:T65 Arg2:T66\n" + "R43	has_short_form Arg1:T33 Arg2:T34\n"
				+ "R44	has_short_form Arg1:T121 Arg2:T122\n" + "R45	has_short_form Arg1:T3 Arg2:T4\n"
				+ "R46	has_short_form Arg1:T167 Arg2:T168\n" + "R47	has_short_form Arg1:T7 Arg2:T8\n"
				+ "R48	has_short_form Arg1:T117 Arg2:T118\n" + "R49	has_short_form Arg1:T57 Arg2:T58\n"
				+ "R50	has_short_form Arg1:T161 Arg2:T162\n" + "R51	has_short_form Arg1:T28 Arg2:T27\n"
				+ "R52	has_short_form Arg1:T50 Arg2:T49\n" + "R53	has_short_form Arg1:T137 Arg2:T138\n"
				+ "R54	has_short_form Arg1:T103 Arg2:T104\n" + "R55	has_short_form Arg1:T21 Arg2:T22\n"
				+ "R56	has_short_form Arg1:T81 Arg2:T82\n" + "R57	has_short_form Arg1:T42 Arg2:T41\n"
				+ "R58	has_short_form Arg1:T111 Arg2:T112\n" + "R59	has_short_form Arg1:T173 Arg2:T174\n"
				+ "R60	has_short_form Arg1:T90 Arg2:T89\n" + "R61	has_short_form Arg1:T95 Arg2:T96\n"
				+ "R62	has_short_form Arg1:T179 Arg2:T180\n" + "R63	has_short_form Arg1:T97 Arg2:T98\n"
				+ "R64	has_short_form Arg1:T26 Arg2:T25\n" + "R65	has_short_form Arg1:T75 Arg2:T76\n"
				+ "R66	has_short_form Arg1:T44 Arg2:T43\n" + "R67	has_short_form Arg1:T32 Arg2:T31\n"
				+ "R68	has_short_form Arg1:T172 Arg2:T171\n" + "R69	has_short_form Arg1:T184 Arg2:T183\n"
				+ "R70	has_short_form Arg1:T101 Arg2:T102\n" + "R71	has_short_form Arg1:T69 Arg2:T70\n"
				+ "R72	has_short_form Arg1:T92 Arg2:T91\n" + "R73	has_short_form Arg1:T169 Arg2:T170\n"
				+ "R74	has_short_form Arg1:T87 Arg2:T88\n" + "R75	has_short_form Arg1:T146 Arg2:T145\n"
				+ "R76	has_short_form Arg1:T177 Arg2:T178\n" + "R77	has_short_form Arg1:T133 Arg2:T134\n"
				+ "R78	has_short_form Arg1:T47 Arg2:T48\n" + "R79	has_short_form Arg1:T165 Arg2:T166\n"
				+ "R80	has_short_form Arg1:T151 Arg2:T152\n" + "R81	has_short_form Arg1:T51 Arg2:T52\n"
				+ "R82	has_short_form Arg1:T147 Arg2:T148\n" + "R83	has_short_form Arg1:T5 Arg2:T6\n"
				+ "R84	has_short_form Arg1:T71 Arg2:T72\n" + "R85	has_short_form Arg1:T24 Arg2:T23\n"
				+ "R86	has_short_form Arg1:T84 Arg2:T83\n" + "R87	has_short_form Arg1:T141 Arg2:T142\n"
				+ "R88	has_short_form Arg1:T30 Arg2:T29\n" + "R89	has_short_form Arg1:T125 Arg2:T126\n"
				+ "R90	has_short_form Arg1:T99 Arg2:T100\n";

		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();
		TextDocument abbrevDoc = bionlpReader.readDocument("PMID:12345", "example",
				new ByteArrayInputStream(bionlp.getBytes()),
				new FileInputStream(new File(
						"/Users/bill/projects/craft-shared-task/exclude-nested-concepts/craft.git/articles/txt/15615595.txt")),
				CharacterEncoding.UTF_8);

		// make sure each long form has a short form
		Set<TextAnnotation> longFormAnnots = ConceptPostProcessingFn.getLongFormAnnots(abbrevDoc.getAnnotations());
		Set<TextAnnotation> shortFormAnnots = ConceptPostProcessingFn.getShortFormAnnots(longFormAnnots);

		for (TextAnnotation longForm : longFormAnnots) {
			ConceptPostProcessingFn.getShortAbbrevAnnot(longForm);
		}

		// this test passes as long as it doesn't throw an exception for a missing slot
		// filler

		// TODO: investigate long_form annotation missing short_form b/c it's a
		// duplicate annotation, e.g., T153 long_form 20748 20771 interventricular
		// septum\n"
	}

}
