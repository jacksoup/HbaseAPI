package org.caesar.hbase.annotation;


import java.lang.annotation.*;

/**
 * Created by caesar.zhu on 15-9-22.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited()
public @interface HbaseField {


}
