package org.apache.kyuubi.common;


import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = ElementType.FIELD)
public @interface Version {
    String value();

    String V1 = "0.0.1";

    String V20210305 = "20210305";
}
