package org.apache.kyuubi.common;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = ElementType.FIELD)
public @interface Unused {

    String version();

    String desc() default "";
}
