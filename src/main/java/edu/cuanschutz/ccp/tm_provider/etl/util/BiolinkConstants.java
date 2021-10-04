package edu.cuanschutz.ccp.tm_provider.etl.util;

import lombok.Data;

public class BiolinkConstants {
	public static final String GENE_PLACEHOLDER = "@GENE$";
	public static final String CHEMICAL_PLACEHOLDER = "@CHEMICAL$";
	public static final String DISEASE_PLACEHOLDER = "@DISEASE$";
	public static final String LOCATION_PLACEHOLDER = "@LOCATION$";
	public static final String PHENOTYPIC_FEATURE_PLACEHOLDER = "@PHENOTYPICFEATURE$";

	private static final String REGULATING_GENE_PLACEHOLDER = "@GENE_REGULATOR$";
	private static final String REGULATED_GENE_PLACEHOLDER = "@REGULATED_GENE$";

	private static final String BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE_ASSOCIATION = "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation";
	private static final String BL_CHEMICAL_TO_GENE_ASSOCIATION = "biolink:ChemicalToGeneAssociation";
	private static final String BL_DISEASE_TO_PHENOTYPIC_FEATURE_ASSOCIATION = "biolink:DiseaseToPhenotypicFeatureAssociation";
	private static final String BL_GENE_REGULATORY_RELATIONSHIP_ASSOCIATION = "biolink:GeneRegulatoryRelationship";
	private static final String BL_GENE_TO_DISEASE_ASSOCIATION = "biolink:GeneToDiseaseAssociation";
	private static final String BL_GENE_TO_EXPRESSION_SITE_ASSOCIATION = "biolink:GeneToExpressionSiteAssociation";

	public enum BiolinkAssociation {
		BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE(BL_CHEMICAL_TO_DISEASE_OR_PHENOTYPIC_FEATURE_ASSOCIATION,
				CHEMICAL_PLACEHOLDER, DISEASE_PLACEHOLDER,
				new SPO[] { new SPO(CHEMICAL_PLACEHOLDER, BiolinkPredicate.BL_TREATS, DISEASE_PLACEHOLDER),
//						new SPO(CHEMICAL_PLACEHOLDER, BiolinkPredicate.BL_CONTRIBUTES_TO, DISEASE_PLACEHOLDER),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_CHEMICAL_TO_GENE(BL_CHEMICAL_TO_GENE_ASSOCIATION, CHEMICAL_PLACEHOLDER, GENE_PLACEHOLDER, new SPO[] {
				new SPO(CHEMICAL_PLACEHOLDER, BiolinkPredicate.BL_ENTITY_POSITIVELY_REGULATES_ENTITY, GENE_PLACEHOLDER),
				new SPO(CHEMICAL_PLACEHOLDER, BiolinkPredicate.BL_ENTITY_NEGATIVELY_REGULATES_ENTITY, GENE_PLACEHOLDER),
				new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_DISEASE_TO_PHENOTYPIC_FEATURE(BL_DISEASE_TO_PHENOTYPIC_FEATURE_ASSOCIATION, DISEASE_PLACEHOLDER,
				PHENOTYPIC_FEATURE_PLACEHOLDER,
				new SPO[] {
						new SPO(DISEASE_PLACEHOLDER, BiolinkPredicate.BL_HAS_PHENOTYPE, PHENOTYPIC_FEATURE_PLACEHOLDER),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_GENE_REGULATORY_RELATIONSHIP(BL_GENE_REGULATORY_RELATIONSHIP_ASSOCIATION, REGULATING_GENE_PLACEHOLDER,
				REGULATED_GENE_PLACEHOLDER,
				new SPO[] {
						new SPO(REGULATING_GENE_PLACEHOLDER, BiolinkPredicate.BL_ENTITY_POSITIVELY_REGULATES_ENTITY,
								REGULATED_GENE_PLACEHOLDER),
						new SPO(REGULATING_GENE_PLACEHOLDER, BiolinkPredicate.BL_ENTITY_NEGATIVELY_REGULATES_ENTITY,
								REGULATED_GENE_PLACEHOLDER),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_GENE_TO_DISEASE(BL_GENE_TO_DISEASE_ASSOCIATION, GENE_PLACEHOLDER, DISEASE_PLACEHOLDER,
				new SPO[] { new SPO(GENE_PLACEHOLDER, BiolinkPredicate.BL_CONTRIBUTES_TO, DISEASE_PLACEHOLDER),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_GENE_LOSS_GAIN_OF_FUNCTION_TO_DISEASE(BL_GENE_TO_DISEASE_ASSOCIATION, GENE_PLACEHOLDER, DISEASE_PLACEHOLDER,
				new SPO[] {
						new SPO(GENE_PLACEHOLDER, BiolinkPredicate.BL_LOSS_OF_FUNCTION_CONTRIBUTES_TO,
								DISEASE_PLACEHOLDER),
						new SPO(GENE_PLACEHOLDER, BiolinkPredicate.BL_GAIN_OF_FUNCTION_CONTRIBUTES_TO,
								DISEASE_PLACEHOLDER),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

		BL_GENE_TO_EXPRESSION_SITE(BL_GENE_TO_EXPRESSION_SITE_ASSOCIATION, GENE_PLACEHOLDER, LOCATION_PLACEHOLDER,
				new SPO[] { new SPO(GENE_PLACEHOLDER, BiolinkPredicate.BL_EXPRESSED_IN, LOCATION_PLACEHOLDER),
						new SPO(null, BiolinkPredicate.NO_RELATION_PRESENT, null) }),

//		TODO: need to define what kinds of assertions are extracted here
//		BL_GENE_TO_GO_TERM(BL_GENE_TO_GO_TERM_ASSOCIATION, new BiolinkPredicate[] {})

		;

		/**
		 * relations details the relations (and their order) that are returned in the
		 * BERT output. These strings should correspond to the relations (and order)
		 * specified in our custom version of BlueBERT (e.g.
		 * https://github.com/UCDenver-ccp/bluebert/blob/6a91a1bae7bd911cb32dca7a52379680499d213a/bluebert/run_bluebert.py#L249)
		 */
		private final SPO[] spoTriples;
		private final String subjectPlaceholder;
		private final String objectPlaceholder;

		private final String associationId;

		private BiolinkAssociation(String associationId, String subjectPlaceholder, String objectPlaceholder,
				SPO[] spoTriples) {
			this.associationId = associationId;
			this.subjectPlaceholder = subjectPlaceholder;
			this.objectPlaceholder = objectPlaceholder;
			this.spoTriples = spoTriples;
		}

		public String getAssociationId() {
			return this.associationId;
		}

		public String getSubjectPlaceholder() {
			return this.subjectPlaceholder;
		}

		public String getObjectPlaceholder() {
			return this.objectPlaceholder;
		}

		public SPO[] getSpoTriples() {
			return this.spoTriples;
		}
	}

	public enum BiolinkPredicate {
		/**
		 * This is a placeholder for the column that is typically labeled "false" in the
		 * BERT output to signify that no relation was predicted.
		 */
		NO_RELATION_PRESENT(null, "false"),
		BL_ENTITY_POSITIVELY_REGULATES_ENTITY("biolink:entity_positively_regulates_entity", "pos-reg"),
		BL_ENTITY_NEGATIVELY_REGULATES_ENTITY("biolink:entity_negatively_regulates_entity", "neg-reg"),
		BL_TREATS("biolink:treats", "treats"), BL_EXPRESSED_IN("biolink:expressed_in", "expressed_in"),
		BL_CONTRIBUTES_TO("biolink:contributes_to", "contributes_to"),
		BL_LOSS_OF_FUNCTION_CONTRIBUTES_TO("biolink:loss_of_function_contributes_to",
				"contributes_to_via_loss_of_function"),
		BL_GAIN_OF_FUNCTION_CONTRIBUTES_TO("biolink:gain_of_function_contributes_to",
				"contributes_to_via_gain_of_function"),

		BL_HAS_PHENOTYPE("biolink:has_phenotype", "has_phenotype");

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
		private final String subjPlaceholder;
		private final BiolinkPredicate predicate;
		private final String objPlaceholder;
	}
}
