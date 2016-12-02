package org.caesar.hbase.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by caesar.zhu on 15-9-22.
 * 目前对hbase中family(列簇)的设计原则是每个表（model）使用一个family
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HbaseDocument {
    String table() ;
    String family() default "#DEFAULT_FAMILY#";
}
