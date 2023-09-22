package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import lombok.Data;

public class BiolinkConstants {

	public static final String ANATOMICAL_SITE_PLACEHOLDER = "@SITE$";
	public static final String CELL_TYPE_PLACEHOLDER = "@CELL$";
	public static final String CELLULAR_COMPONENT_PLACEHOLDER = "@COMPONENT$";
	public static final String GENE_PLACEHOLDER = "@GENE$";
	public static final String CHEMICAL_PLACEHOLDER = "@CHEMICAL$";
	public static final String DISEASE_PLACEHOLDER = "@DISEASE$";
	public static final String ANY_LOCATION_PLACEHOLDER = "@LOCATION$";
	public static final String PHENOTYPIC_FEATURE_PLACEHOLDER = "@PHENOTYPICFEATURE$";
	public static final String PROCESS_PLACEHOLDER = "@PROCESS$";

	private static final String REGULATING_GENE_PLACEHOLDER = "@GENE_REGULATOR$";
	private static final String REGULATED_GENE_PLACEHOLDER = "@REGULATED_GENE$";

	private static final String BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE_ASSOCIATION = "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation";
	private static final String BL_CHEMICAL_TO_GENE_ASSOCIATION = "biolink:ChemicalToGeneAssociation";
	private static final String BL_DISEASE_TO_PHENOTYPIC_FEATURE_ASSOCIATION = "biolink:DiseaseToPhenotypicFeatureAssociation";
	private static final String BL_DISEASE_OR_PHENOTYPIC_FEATURE_TO_LOCATION_ASSOCIATION = "biolink:DiseaseOrPhenotypicFeatureToLocationAssociation";
	private static final String BL_GENE_REGULATORY_RELATIONSHIP_ASSOCIATION = "biolink:GeneRegulatoryRelationship";
	private static final String BL_GENE_TO_DISEASE_ASSOCIATION = "biolink:GeneToDiseaseAssociation";
	private static final String BL_GENE_TO_CELLULAR_COMPONENT_ASSOCIATION = "biolink:GeneToCellularComponentAssociation"; // TODO:
																															// This
																															// Association
																															// needs
																															// to
																															// be
																															// added
																															// to
																															// Biolink
	private static final String BL_GENE_TO_CELL_ASSOCIATION = "biolink:GeneToCellAssociation"; // TODO: This Association
																								// needs to be added to
																								// Biolink
	private static final String BL_GENE_TO_ANATOMICAL_ENTITY_ASSOCIATION = "biolink:GeneToAnatomalEntityAssociation"; // TODO:
																														// This
																														// Association
																														// needs
																														// to
																														// be
																														// added
																														// to
																														// Biolink
	private static final String BL_GENE_TO_BIOLOGICAL_PROCESS_ASSOCIATION = "biolink:GeneToBiologicalProcessAssociation";

	// TODO: THis is not yet an official Biolink Association
	private static final String BL_BIOLOGICAL_PROCESS_TO_DISEASE_OR_PHENOTYPIC_FEATURE_ASSOCIATION = "biolink:BiologicalProcessToDiseaseOrPhenotypicFeatureAssociation";

	public enum BiolinkAssociation {
		// @formatter:off
		BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE(BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE_ASSOCIATION,
				BiolinkClass.CHEMICAL, BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE,
				new SPO[] { new SPO(BiolinkClass.CHEMICAL, BiolinkPredicate.BL_TREATS, BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE),
						new SPO(BiolinkClass.CHEMICAL, BiolinkPredicate.BL_CONTRIBUTES_TO, BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_CHEMICAL_TO_GENE(BL_CHEMICAL_TO_GENE_ASSOCIATION, BiolinkClass.CHEMICAL, BiolinkClass.GENE,
				new SPO[] {
						new SPO(BiolinkClass.CHEMICAL, BiolinkPredicate.BL_ENTITY_POSITIVELY_REGULATES_ENTITY, BiolinkClass.GENE),
						new SPO(BiolinkClass.CHEMICAL, BiolinkPredicate.BL_ENTITY_NEGATIVELY_REGULATES_ENTITY, BiolinkClass.GENE),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_DISEASE_TO_PHENOTYPIC_FEATURE(BL_DISEASE_TO_PHENOTYPIC_FEATURE_ASSOCIATION, BiolinkClass.DISEASE,
				BiolinkClass.PHENOTYPIC_FEATURE,
				new SPO[] {
						new SPO(BiolinkClass.DISEASE, BiolinkPredicate.BL_HAS_PHENOTYPE, BiolinkClass.PHENOTYPIC_FEATURE),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_GENE_REGULATORY_RELATIONSHIP(BL_GENE_REGULATORY_RELATIONSHIP_ASSOCIATION, BiolinkClass.REGULATING_GENE,
				BiolinkClass.REGULATED_GENE,
				new SPO[] {
						new SPO(BiolinkClass.REGULATING_GENE, BiolinkPredicate.BL_ENTITY_POSITIVELY_REGULATES_ENTITY, BiolinkClass.REGULATED_GENE),
						new SPO(BiolinkClass.REGULATING_GENE, BiolinkPredicate.BL_ENTITY_NEGATIVELY_REGULATES_ENTITY, BiolinkClass.REGULATED_GENE),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_GENE_TO_DISEASE(BL_GENE_TO_DISEASE_ASSOCIATION, BiolinkClass.GENE,
				BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE,
				new SPO[] {
						new SPO(BiolinkClass.GENE, BiolinkPredicate.BL_CONTRIBUTES_TO, BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_GENE_LOSS_GAIN_OF_FUNCTION_TO_DISEASE(BL_GENE_TO_DISEASE_ASSOCIATION, BiolinkClass.GENE,
				BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE,
				new SPO[] {
						new SPO(BiolinkClass.GENE, BiolinkPredicate.BL_LOSS_OF_FUNCTION_CONTRIBUTES_TO, BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE),
						new SPO(BiolinkClass.GENE, BiolinkPredicate.BL_GAIN_OF_FUNCTION_CONTRIBUTES_TO, BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_GENE_TO_CELLULAR_COMPONENT(BL_GENE_TO_CELLULAR_COMPONENT_ASSOCIATION, BiolinkClass.GENE, BiolinkClass.CELLULAR_COMPONENT,
				new SPO[] { new SPO(BiolinkClass.GENE, BiolinkPredicate.BL_LOCATED_IN, BiolinkClass.CELLULAR_COMPONENT),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),
		
		BL_GENE_TO_CELL(BL_GENE_TO_CELL_ASSOCIATION, BiolinkClass.GENE, BiolinkClass.CELL_TYPE,
				new SPO[] { 
						new SPO(BiolinkClass.GENE, BiolinkPredicate.BL_LOCATED_IN, BiolinkClass.CELL_TYPE),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),
		
		BL_GENE_TO_ANATOMICAL_ENTITY(BL_GENE_TO_ANATOMICAL_ENTITY_ASSOCIATION, BiolinkClass.GENE, BiolinkClass.ANATOMICAL_SITE,
				new SPO[] { new SPO(BiolinkClass.GENE, BiolinkPredicate.BL_LOCATED_IN, BiolinkClass.ANATOMICAL_SITE),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_DISEASE_OR_PHENOTYPIC_FEATURE_TO_LOCATION(BL_DISEASE_OR_PHENOTYPIC_FEATURE_TO_LOCATION_ASSOCIATION,
				BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE, BiolinkClass.ANATOMICAL_SITE,
				new SPO[] {
						new SPO(BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE, BiolinkPredicate.BL_OCCURS_IN, BiolinkClass.ANATOMICAL_SITE),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_BIOLOGICAL_PROCESS_TO_DISEASE(BL_BIOLOGICAL_PROCESS_TO_DISEASE_OR_PHENOTYPIC_FEATURE_ASSOCIATION,
				BiolinkClass.BIOLOGICAL_PROCESS, BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE,
				new SPO[] {
						new SPO(BiolinkClass.BIOLOGICAL_PROCESS, BiolinkPredicate.BL_ACTIVELY_INVOLVED_IN, BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),
		
		BL_GENE_TO_BIOLOGICAL_PROCESS(BL_GENE_TO_BIOLOGICAL_PROCESS_ASSOCIATION,
				BiolinkClass.GENE, BiolinkClass.BIOLOGICAL_PROCESS, 
				new SPO[] {
						new SPO(BiolinkClass.GENE, BiolinkPredicate.BL_PARTICIPATES_IN, BiolinkClass.BIOLOGICAL_PROCESS),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),
		// @formatter:on

		;

		/**
		 * relations details the relations (and their order) that are returned in the
		 * BERT output. These strings should correspond to the relations (and order)
		 * specified in our custom version of BlueBERT (e.g.
		 * https://github.com/UCDenver-ccp/bluebert/blob/6a91a1bae7bd911cb32dca7a52379680499d213a/bluebert/run_bluebert.py#L249)
		 */
		private final SPO[] spoTriples;
		private final BiolinkClass subjectClass;
		private final BiolinkClass objectClass;

		private final String associationId;

		private BiolinkAssociation(String associationId, BiolinkClass subjectClass, BiolinkClass objectClass,
				SPO[] spoTriples) {
			this.associationId = associationId;
			this.subjectClass = subjectClass;
			this.objectClass = objectClass;
			this.spoTriples = spoTriples;
		}

		public String getAssociationId() {
			return this.associationId;
		}

		public BiolinkClass getSubjectClass() {
			return this.subjectClass;
		}

		public BiolinkClass getObjectClass() {
			return this.objectClass;
		}

		public SPO[] getSpoTriples() {
			return this.spoTriples;
		}
	}

	public enum BiolinkPredicate {
		// @formatter:off
		/**
		 * This is a placeholder for the column that is typically labeled "false" in the
		 * BERT output to signify that no relation was predicted.
		 */
		NO_RELATION_PRESENT(null, "false"),
		BL_ENTITY_POSITIVELY_REGULATES_ENTITY("biolink:entity_positively_regulates_entity", "pos-reg"),
		BL_ENTITY_NEGATIVELY_REGULATES_ENTITY("biolink:entity_negatively_regulates_entity", "neg-reg"),
		BL_TREATS("biolink:treats", "treats"), 
		BL_EXPRESSED_IN("biolink:expressed_in", "expressed_in"),
		BL_CONTRIBUTES_TO("biolink:contributes_to", "contributes_to"),
		BL_LOSS_OF_FUNCTION_CONTRIBUTES_TO("biolink:loss_of_function_contributes_to",
				"contributes_to_via_loss_of_function"),
		BL_GAIN_OF_FUNCTION_CONTRIBUTES_TO("biolink:gain_of_function_contributes_to",
				"contributes_to_via_gain_of_function"),
		BL_HAS_PHENOTYPE("biolink:has_phenotype", "has_phenotype"), 
		BL_OCCURS_IN("biolink:occurs_in", "occurs_in"),
		BL_ACTIVELY_INVOLVED_IN("biolink:actively_involved_in", "actively_involved_in"),
		BL_PARTICIPATES_IN("biolink:participates_in", "participates_in"),
		BL_LOCATED_IN("biolink:located_in", "located_in");
		// @formatter:on

		private final String curie;
		private final String labelAbbreviation;

		private BiolinkPredicate(String curie, String labelAbbreviation) {
			this.curie = curie;
			this.labelAbbreviation = labelAbbreviation;
		}

		public String getCurie() {
			return this.curie;
		}

		public String getEdgeLabelAbbreviation() {
			return this.labelAbbreviation;
		}
	}

	@Data
	public static class SPO {
		private final BiolinkClass subjClass;
		private final BiolinkPredicate predicate;
		private final BiolinkClass objClass;
	}

	public enum BiolinkClass {
		// @formatter:off
		DISEASE(DISEASE_PLACEHOLDER, "MONDO"), 
		DISEASE_OR_PHENOTYPIC_FEATURE(DISEASE_PLACEHOLDER, "MONDO", "HP"),
		PHENOTYPIC_FEATURE(PHENOTYPIC_FEATURE_PLACEHOLDER, "HP"), 
		CHEMICAL(CHEMICAL_PLACEHOLDER, "DRUGBANK", "CHEBI"),
		BIOLOGICAL_PROCESS(PROCESS_PLACEHOLDER, "GO"),  // TODO: won't work until Elastic is updated to split GO into component hierarchies
		ANATOMICAL_SITE(ANATOMICAL_SITE_PLACEHOLDER, "UBERON"),
		CELLULAR_COMPONENT(CELLULAR_COMPONENT_PLACEHOLDER, "GO"), // TODO: won't work until Elastic is updated to split GO into component hierarchies
		CELL_TYPE(CELL_TYPE_PLACEHOLDER, "CL"),
		ANY_LOCATION(ANY_LOCATION_PLACEHOLDER, "UBERON", "CL", "GO"), // TODO: won't work until Elastic is updated to split GO into component hierarchies
		GENE(GENE_PLACEHOLDER, "PR"),
		REGULATED_GENE(REGULATED_GENE_PLACEHOLDER, "PR"), 
		REGULATING_GENE(REGULATING_GENE_PLACEHOLDER, "PR");
		// @formatter:on

		private final String placeholder;
		private final String[] ontologyPrefixes;

		private BiolinkClass(String placeholder, String... ontologyPrefixes) {
			this.placeholder = placeholder;
			this.ontologyPrefixes = ontologyPrefixes;
		}

		public Set<String> getOntologyPrefixes() {
			return new HashSet<String>(Arrays.asList(ontologyPrefixes));
		}

		public String getPlaceholder() {
			return this.placeholder;
		}

	}
}
