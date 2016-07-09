package ch.heia.pop.yarn.example;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import static popjava.util.ClassUtil.isAssignableFrom;

/**
 *
 * @author Dosky
 */
public class Test {
    
    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Class clazz = Class.forName("ch.heia.pop.yarn.example.NoSpec");
        Method method = clazz.getDeclaredMethod("main", String[].class);
        method.invoke(null, args);
    }

    public static Constructor<?> getConstructor(Class<?> c,
            Class<?>... parameterTypes) throws NoSuchMethodException {

        Constructor<?>[] allConstructors = c.getConstructors();
        for (Constructor<?> constructor : allConstructors) {
            if (isSameConstructor(constructor, parameterTypes)) {
                return constructor;
            }
        }

        String sign = getMethodSign(c.getName(), parameterTypes);
        String errorMessage = String.format(
                "Cannot find the method %s in class %s", sign, c.getName());
        throw new NoSuchMethodException(errorMessage);

    }

    public static String getMethodSign(Constructor<?> c) {
        return getMethodSign(c.getDeclaringClass().getName(), c
                .getParameterTypes());
    }

    public static String getMethodSign(String name, Class<?>[] parameterTypes) {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        for (Class<?> c : parameterTypes) {
            sb.append("-");
            sb.append(getClassName(c));
        }
        return sb.toString();
    }

    private static boolean isSameConstructor(Constructor<?> constructor,
            Class<?>[] params) {
        if (params == null) {
            return false;
        }
        Class<?>[] parameters = constructor.getParameterTypes();
        return areParameterTypesTheSame(params, parameters);
    }

    public static boolean areParameterTypesTheSame(Class<?>[] params,
            Class<?>[] constructorParameters) {
        if (constructorParameters.length > params.length
                || (constructorParameters.length == 0 && params.length > 0)) {
            return false;
        }
        for (int index = 0; index < constructorParameters.length; index++) {

            if (index == constructorParameters.length - 1) {
                if (isAssignableFrom(constructorParameters[index], params[index])) {
                    if (constructorParameters.length == params.length) {
                        return true;
                    } else {
                        return false;
                    }
                } else if (constructorParameters[index].isArray()) {

                    Class<?> componentClass = constructorParameters[index].getComponentType();
                    for (int i = index; i < params.length; i++) {
                        if (!isAssignableFrom(componentClass, params[i])) {
                            return false;
                        }
                    }

                    return true;
                } else {
                    return false;
                }
            } else if (!isAssignableFrom(constructorParameters[index], params[index])) {
                return false;
            }

        }
        return true;
    }

    private static String getClassName(Class<?> c) {
        if (c == byte.class) {
            return Byte.class.getName();
        }
        if (c == int.class) {
            return Integer.class.getName();
        }
        if (c == short.class) {
            return Short.class.getName();
        }
        if (c == long.class) {
            return Long.class.getName();
        }
        if (c == float.class) {
            return Float.class.getName();
        }
        if (c == double.class) {
            return Double.class.getName();
        }
        if (c == boolean.class) {
            return Boolean.class.getName();
        }
        if (c == char.class) {
            return Character.class.getName();
        }

        return c.getName();

    }
}