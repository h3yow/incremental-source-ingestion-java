package com.github.h3yow.incrementalsourceingestionjava.normalize;

import com.github.h3yow.incrementalsourceingestionjava.model.NormalizationResult;
import com.github.h3yow.incrementalsourceingestionjava.model.RawRecord;

/**
 * 归一器：把来源原始记录转为统一格式，或给出明确拒绝原因。
 */
@FunctionalInterface
public interface RecordNormalizer {
    NormalizationResult normalize(RawRecord raw);
}
