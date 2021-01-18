package edu.cuanschutz.ccp.tm_provider.kg;

import java.util.HashSet;
import java.util.Set;

import edu.ucdenver.ccp.common.digest.DigestUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public class KgxEdge {

	public enum EvidenceMode {
		WRITE_PLACEHOLDER, WRITE_EVIDENCE
	}

	public static final String EVIDENCE_ID_PLACEHOLDER = "EVIDENCE_ID_PLACEHOLDER";
	public static final String EVIDENCE_COUNT_PLACEHOLDER = "EVIDENCE_COUNT_PLACEHOLDER";

	private final String biolinkAssociationType;
	private final String subjectCurie;
	private final String edgeLabel;
	private final String objectCurie;
	private final String relationCurie;
	@EqualsAndHashCode.Exclude
	private Set<KgxNlpEvidenceNode> evidenceNodes;

	public String getId() {
		return DigestUtil.getBase64Sha1Digest(String.format("%s|%s|%s|%s|%s", subjectCurie, edgeLabel, objectCurie,
				relationCurie, biolinkAssociationType));
	}

	public void addEvidenceNode(KgxNlpEvidenceNode node) {
		if (evidenceNodes == null) {
			evidenceNodes = new HashSet<KgxNlpEvidenceNode>();
		}
		if (!evidenceNodes.contains(node)) {
			evidenceNodes.add(node);
		}
	}

	public String toKgxString(EvidenceMode mode) {
		StringBuilder evidenceNodeIdListBuilder = new StringBuilder();
		String evidenceCountStr = "";
		if (mode == EvidenceMode.WRITE_EVIDENCE) {
			if (getEvidenceNodes() != null && !getEvidenceNodes().isEmpty()) {
				for (KgxNlpEvidenceNode evidenceNode : getEvidenceNodes()) {
					evidenceNodeIdListBuilder.append(evidenceNode.getId() + "|");
				}
				// remove trailing pipe
				evidenceNodeIdListBuilder.deleteCharAt(evidenceNodeIdListBuilder.length() - 1);
				evidenceCountStr = Integer.toString(getEvidenceNodes().size());
			}
		} else if (mode == EvidenceMode.WRITE_PLACEHOLDER) {
			evidenceNodeIdListBuilder.append(EVIDENCE_ID_PLACEHOLDER);
			evidenceCountStr = EVIDENCE_COUNT_PLACEHOLDER;
		} else {
			throw new IllegalArgumentException("Unhandled evidence mode: " + mode.name());
		}

		String s = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", subjectCurie, edgeLabel, objectCurie, relationCurie,
				getId(), biolinkAssociationType, evidenceCountStr, evidenceNodeIdListBuilder.toString());
		return s;
	}

}