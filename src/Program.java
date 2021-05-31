import com.configuration.Configuration;
import com.models.ConnectionInfo;
import com.models.PubSubTableInfo;
import com.publisher.ChangeDataCapture;
import com.publisher.Publication;
import com.subscriber.DataListener;
import com.subscriber.SettingUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Program {
    private static final Logger logger = LogManager.getLogger(Program.class);

    public static void main(String[] args) {
        Configurator.initialize(null, "resources/log4j2.xml");

        ConnectionInfo publisherConnectionInfo;
        ConnectionInfo subscriberConnectionInfo;
        List<PubSubTableInfo> pubSubTableInfoList;

        String publicationName;
        String slot;
        int maxTasks;
        int batchSize;

        try {
            System.out.println("reading the configuration data ...");

            com.configuration.Configuration configuration = new Configuration();
            publisherConnectionInfo = configuration.getPublisherConnectionInfo();
            subscriberConnectionInfo = configuration.getSubscriberConnectionInfo();
            pubSubTableInfoList = configuration.getPubSubTableInfoList();

            publicationName = configuration.getPublicationName();
            slot = configuration.getPublisherSlot();

            maxTasks = configuration.getMaxTasks();
            batchSize = configuration.getBatchSize();

            System.out.println("done.");
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("main() - {}", ex.toString());
            return;
        }

        try {
            System.out.println("setting up...");
            try (SettingUp settingUp = new SettingUp(publisherConnectionInfo, subscriberConnectionInfo, pubSubTableInfoList)) {
                settingUp.createSubscriberTables();
            }
            System.out.println("done.");
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("main() - {}", ex.toString());
            return;
        }

        try {
            System.out.println("publication ...");
            try (Publication publication = new Publication(publisherConnectionInfo, publicationName, pubSubTableInfoList)) {
                publication.initializePublication();
            }
            System.out.println("done.");
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("main() - {}", ex.toString());
            return;
        }

        ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> concurrentHashMap = new ConcurrentHashMap<>();
        ChangeDataCapture changeDataCapture = null;
        DataListener dataListener = null;
        try {
            System.out.println("starting the change data capture process ...");
            changeDataCapture = new ChangeDataCapture(concurrentHashMap, publisherConnectionInfo, publicationName, slot, true, true, false);
            changeDataCapture.start();
            System.out.println("done.");

            System.out.println("trying to start the data listener ...");
            ConcurrentLinkedQueue<String> queue = concurrentHashMap.get(publisherConnectionInfo.getDatabase());
            dataListener = new DataListener(queue, maxTasks, batchSize, subscriberConnectionInfo, pubSubTableInfoList);
            dataListener.start();
            System.out.println("done.");

            while (true) {
                System.out.println("\r\npress key:");
                System.out.println("'q' - to exit");
                System.out.println("'i' - to get the count of handled records");
                Scanner scanner = new Scanner(System.in);
                char c = scanner.next().charAt(0);
                if (c == 'q' || c == 'Q') {
                    break;
                } else if (c == 'i' || c == 'I') {
                    System.out.printf("\r\n%d records handled\r\n", dataListener.getRecordsCount());
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("main() - {}", ex.toString());
        } finally {
            if (dataListener != null) {
                System.out.println("\r\ntrying to interrupt the work of DataListener class...");
                dataListener.interrupt();

                while (dataListener.isAlive() == true) {
                    try {
                        Thread.sleep(10); // Sleep 10 millis
                    } catch (InterruptedException ex) {
                    }
                }

                System.out.println("\r\ndone.");
            }

            if (changeDataCapture != null) {
                System.out.println("\r\ntrying to interrupt the work of ChangeDataCapture class...");
                changeDataCapture.interrupt();

                while (changeDataCapture.isAlive() == true) {
                    try {
                        Thread.sleep(10); // Sleep 10 millis
                    } catch (InterruptedException ex) {
                    }
                }

                System.out.println("\r\ndone.");
            }
        }

        System.out.println("\r\nbye, bye...");
    }
}
