package edu.cuanschutz.ccp.tm_provider.etl.fn;

import com.google.datastore.v1.Entity;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static edu.cuanschutz.ccp.tm_provider.etl.BiorxivXmlToTextPipeline.LOGGER;
import static edu.cuanschutz.ccp.tm_provider.etl.fn.DocumentToEntityFn.createEntity;

@Data
@EqualsAndHashCode(callSuper = false)
public class JatsDocumentToEntityFn extends DoFn<KV<String, List<String>>, Entity> {
    private static final long serialVersionUID = 1L;
    private final DocumentCriteria dc;
    private final PCollectionView<Map<String, Set<String>>> documentIdToCollectionsMapView;

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<String, List<String>> documentKV = c.element();
        if (documentKV != null) {
            String docId = documentKV.getKey();
            List<String> documentContents = documentKV.getValue();

            Map<String, Set<String>> docIdToCollectionsMap = c.sideInput(documentIdToCollectionsMapView);
            Set<String> collections = new HashSet<>();

            if (docIdToCollectionsMap != null && docIdToCollectionsMap.containsKey(docId)) {
                collections.addAll(docIdToCollectionsMap.get(docId));
            }

            if (documentContents != null) {
                int index = 0;
                for (String docContent : documentContents) {
                    try {
                        Entity entity = createEntity(docId, index++, documentContents.size(), dc, docContent, collections);
                        c.output(entity);
                    } catch (UnsupportedEncodingException ex) {
                        LOGGER.warning(ex.getMessage());
                    }
                }
            }
        }
    }
}
