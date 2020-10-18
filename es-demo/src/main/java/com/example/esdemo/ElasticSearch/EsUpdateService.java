package com.example.esdemo.ElasticSearch;

import com.example.esdemo.bean.CustomerEsSelectBean;
import com.example.esdemo.config.EsConfig;
import com.example.esdemo.utils.EsUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 更新
 *
 * @author WangBoran
 * @since 2020/9/1 17:03
 */
@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class EsUpdateService {

    private final RestHighLevelClient restHighLevelClient;

    private final EsConfig esConfig;


}
