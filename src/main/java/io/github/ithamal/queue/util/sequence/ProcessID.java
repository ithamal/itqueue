package io.github.ithamal.queue.util.sequence;

import org.springframework.util.StringUtils;

import java.lang.management.ManagementFactory;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class ProcessID {

    private static int processId;

    static {
        final String propertiesName = "ms-sms.id";
        String value = null;
        //解决在docker中运行，进程号都一样的问题
        try {
            if (System.getSecurityManager() == null) {
                value = System.getProperty(propertiesName);
            } else {
                value = AccessController.doPrivileged(new PrivilegedAction<String>() {
                    @Override
                    public String run() {
                        return System.getProperty(propertiesName);
                    }
                });
            }
        } catch (SecurityException e) {
        }
        //没有配置gateid就取程序进程号
        if(!StringUtils.hasText(value)) {
            String vmName = ManagementFactory.getRuntimeMXBean().getName();
            if(vmName.contains("@")){
                value =vmName.split("@")[0];
            }
        }

        try{
            processId = Integer.valueOf(value);
        }catch(Exception e){

        }
    }

    public static int getProcessId() {
        return processId;
    }
}
