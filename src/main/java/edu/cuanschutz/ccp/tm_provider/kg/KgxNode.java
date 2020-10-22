package edu.cuanschutz.ccp.tm_provider.kg;

import lombok.Data;

@Data
public class KgxNode implements Comparable<KgxNode> {
	private final String id;
	private final String name;
	private final String category;

	public KgxNode(String id, String name, String category) {
		this.id = id;
		this.name = name;
		this.category = category;
	}

	public String toKgxString(int columnCount) {
		StringBuilder s = new StringBuilder(String.format("%s\t%s\t%s", getId(), getName(), getCategory()));
		for (int i = 3; i < columnCount; i++) {
			s.append("\t");
		}
		return s.toString();
	}

	@Override
	public int compareTo(KgxNode node) {
		return this.getId().compareTo(node.getId());
	}
}
