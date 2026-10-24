package com.example.springaiagent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 启动时把 classpath:knowledge/*.md 切块写入内存向量库，对话时按语义检索。
 */
@Service
public class KnowledgeBaseService {

    private static final Logger log = LoggerFactory.getLogger(KnowledgeBaseService.class);

    private static final int CHUNK_SIZE = 700;
    private static final int CHUNK_OVERLAP = 120;

    private final VectorStore vectorStore;
    private final boolean enabled;
    private final int topK;
    private final double similarityThreshold;
    private final ResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();

    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicInteger chunkCount = new AtomicInteger(0);
    private final List<String> sources = new CopyOnWriteArrayList<>();

    public KnowledgeBaseService(
            VectorStore vectorStore,
            @Value("${app.rag.enabled:true}") boolean enabled,
            @Value("${app.rag.top-k:4}") int topK,
            @Value("${app.rag.similarity-threshold:0.5}") double similarityThreshold) {
        this.vectorStore = vectorStore;
        this.enabled = enabled;
        this.topK = topK;
        this.similarityThreshold = similarityThreshold;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void ingest() {
        if (!enabled) {
            log.info("RAG 已通过 app.rag.enabled=false 关闭");
            return;
        }
        try {
            Resource[] resources = resourceResolver.getResources("classpath*:knowledge/*.md");
            List<Document> documents = new ArrayList<>();
            for (Resource resource : resources) {
                String text = resource.getContentAsString(StandardCharsets.UTF_8);
                String filename = resource.getFilename() == null ? "unknown.md" : resource.getFilename();
                documents.add(Document.builder()
                        .text(text)
                        .metadata("source", filename)
                        .build());
                sources.add(filename);
            }

            List<Document> chunks = new ArrayList<>();
            for (Document document : documents) {
                chunks.addAll(chunk(document));
            }
            if (chunks.isEmpty()) {
                log.warn("知识库目录为空：src/main/resources/knowledge/*.md");
                return;
            }

            vectorStore.add(chunks);
            chunkCount.set(chunks.size());
            ready.set(true);
            log.info("知识库加载完成：{} 个文档，{} 个片段", sources.size(), chunks.size());
        } catch (Exception e) {
            // Embedding 服务不可用不应该阻断整个应用启动，降级为纯工具 Agent。
            ready.set(false);
            chunkCount.set(0);
            sources.clear();
            log.warn("知识库初始化失败，RAG 已禁用。请检查 Embedding 模型配置（base-url / api-key / model）：{}", e.toString());
        }
    }

    /**
     * 按字符切块，优先在换行处断开，并保留少量重叠，避免答案被切断。
     * 文档量很小的 Demo 不需要引入完整的分词器。
     */
    private List<Document> chunk(Document document) {
        String text = document.getText() == null ? "" : document.getText().strip();
        String source = String.valueOf(document.getMetadata().getOrDefault("source", "unknown.md"));
        List<Document> chunks = new ArrayList<>();

        int start = 0;
        while (start < text.length()) {
            int end = Math.min(start + CHUNK_SIZE, text.length());
            if (end < text.length()) {
                int boundary = text.lastIndexOf('\n', end);
                if (boundary > start + CHUNK_SIZE / 2) {
                    end = boundary;
                }
            }
            String piece = text.substring(start, end).strip();
            if (!piece.isEmpty()) {
                chunks.add(Document.builder()
                        .text(piece)
                        .metadata("source", source)
                        .build());
            }
            if (end >= text.length()) {
                break;
            }
            start = Math.max(end - CHUNK_OVERLAP, start + 1);
        }
        return chunks;
    }

    public List<Document> search(String query) {
        if (!ready.get() || query == null || query.isBlank()) {
            return List.of();
        }
        try {
            return vectorStore.similaritySearch(SearchRequest.builder()
                    .query(query)
                    .topK(topK)
                    .similarityThreshold(similarityThreshold)
                    .build());
        } catch (Exception e) {
            log.warn("知识库检索失败：{}", e.toString());
            return List.of();
        }
    }

    public boolean isReady() {
        return ready.get();
    }

    public int chunkCount() {
        return chunkCount.get();
    }

    public List<String> sources() {
        return List.copyOf(sources);
    }
}
