package online.nvwa.obase.tools;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FuncScope {
	 /**
     * 作用域枚举
     * @author peida
     * Local: 本地调用
     * Server:调用远程数据端的方法
     * Application:所有公开出去的用于生成对外接口的方法
     *
     */
    public enum Scope{ LOCAL,SERVER,APPLICATION};
    
    /**
     * 颜色属性
     * @return
     */
    Scope value() default Scope.LOCAL;
}
