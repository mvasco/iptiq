
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LoadBalancer {

    // wrapper class for a pool resource
    // containing the Provider object and a status
    // It also includes methods to change the status
    private class ProviderUnit {

        static final int STATUS_OFFLINE = -1;
        //static final int STATUS_CANDIDATE = 0;
        static final int STATUS_ONLINE = 1;

        private final Provider provider;
        private int status;

        ProviderUnit(Provider provider) {
            this.provider = provider;
            status = STATUS_ONLINE;
        }

        void exclude() {
            status = STATUS_OFFLINE;
        }

        void promote() {
            if (status < STATUS_ONLINE) {
                status++;
            }
        }

        void include() {
            status = STATUS_ONLINE;
        }

        int getStatus() {
            return status;
        }

        Provider getProvider() {
            return provider;
        }

        // method used in testing
        public String toString() {
            return "Provider Resource: Provider id:" + provider.getId() + " , Status Code:" + status;
        }
    }

    // enum that holds the different retrieve algorithms available
    enum RetrieveMethod {
        RANDOM,ROUND_ROBIN
    }

    private ProviderUnit[] providerPool; // the provider resource pool
    private final Set<Provider> set; // set of providers available in the load balancer
    private int roundRobin; // helper counter for the round robin retrieval method
    private RetrieveMethod retrieveMethod; // the retrieve method currently set for the load balancer
    private final Semaphore semaphore; // enforces the ceiling on the number of concurrent requests serviced
    private final int heartBeatFrequency; // number of seconds interval for heartbeat
    private final int maxParallelRequests; // number of max parallel requests per provider
    private final Lock poolLock; // lock for access to the resource pool
    private final Lock roundRobinLock; // lock for access to the round robin counter
    private final ScheduledExecutorService es; // executor service for the heartbeat

    public LoadBalancer(RetrieveMethod retrieveMethod, int maxParallelRequests, int heartBeatFrequency) {
        providerPool = new ProviderUnit[0];
        set = new HashSet<>();
        roundRobin = -1; // starts at -1 because round robin algorithm starts by incrementing the counter
        this.retrieveMethod = retrieveMethod;
        semaphore = new Semaphore(0);
        this.maxParallelRequests = maxParallelRequests;
        poolLock = new ReentrantLock();
        roundRobinLock = new ReentrantLock();
        es = Executors.newSingleThreadScheduledExecutor();
        this.heartBeatFrequency = heartBeatFrequency;
    }

    public boolean registerProviders(List<Provider> listToAdd) {
        Set<Provider> addSet = new HashSet<>(listToAdd);
        addSet.removeAll(set); // so we don't have "duplicate providers"
        if (addSet.size() + providerPool.length <= 10) {
            poolLock.lock();
            providerPool =
                    Stream.concat(
                            Arrays.stream(providerPool),
                            addSet.stream().map(ProviderUnit::new))
                    .toArray(ProviderUnit[]::new);
            poolLock.unlock();
            set.addAll(addSet);
            semaphore.release(addSet.size() * maxParallelRequests);
            return true;
        }
        return false;
    }

    private List<Provider> getAvailableProviders() {
        return Arrays.stream(providerPool)
                .filter(p -> p.getStatus() == 1)
                .map(ProviderUnit::getProvider)
                .collect(Collectors.toList());
    }

    private Provider getProvider() {
        poolLock.lock();
        List<Provider> available = getAvailableProviders();
        poolLock.unlock();

        if (available.size() == 0) {
            return null;
        } else if (available.size() == 1) {
            return available.get(0);
        }

        Provider ret = null;
        switch (retrieveMethod) {
            case RANDOM:
                ret = getRandomProvider(available);
                break;
            case ROUND_ROBIN:
                ret = getRoundRobinProvider(available);
                break;
        }
        return ret;
    }

    public String get() {
        Provider provider = null;
        boolean release = false;
        try {
            if (semaphore.tryAcquire()) {
                release = true;
                provider = getProvider();
            }
        } finally {
            if (release) {
                semaphore.release();
            }
        }
        return provider == null ? "<COULD NOT GET>" : provider.get();
    }

    // Step 3 - Random invocation
    private Provider getRandomProvider(List<Provider> availableProviders) {
        return availableProviders.get((int) (Math.random() * availableProviders.size()));
    }

    // Step 4 - Round Robin invocation
    private Provider getRoundRobinProvider(List<Provider> availableProviders) {
        roundRobinLock.lock();
        roundRobin = ++roundRobin % availableProviders.size();
        roundRobinLock.unlock();
        return availableProviders.get(roundRobin);
    }

    // Step 5 - Exclude Provider
    public void excludeProvider(Provider toExclude) {
        poolLock.lock();
        for (ProviderUnit providerUnit : providerPool) {
            if (providerUnit.getProvider() == toExclude) {
                providerUnit.exclude();
                break;
            }
        }
        poolLock.unlock();
    }

    // Step 5 - Include Provider
    public void includeProvider(Provider toInclude) {
        poolLock.lock();
        for (ProviderUnit providerUnit : providerPool) {
            if (providerUnit.getProvider() == toInclude) {
                providerUnit.include();
                break;
            }
        }
        poolLock.unlock();
    }

    // Step 7 - heartbeat excluding and including nodes
    private void heartBeatCheck() {
        int netInclExcl = 0;
        poolLock.lock();
        for (ProviderUnit providerUnit : providerPool) {
            if (!providerUnit.getProvider().check()) {
                providerUnit.exclude();
                netInclExcl--;
            } else if (providerUnit.getStatus() != ProviderUnit.STATUS_ONLINE) {
                providerUnit.promote();
                if (providerUnit.getStatus() == ProviderUnit.STATUS_ONLINE) {
                    netInclExcl++;
                }
            }
        }
        poolLock.unlock();
        if (netInclExcl > 0) {
            try {
                semaphore.acquire((netInclExcl * maxParallelRequests));
            } catch (InterruptedException e) {
                throw new IllegalStateException("Please register providers again");
            }
        } else if (netInclExcl < 0) {
            semaphore.release(-1 * (netInclExcl * maxParallelRequests));
        }
    }

    // Step 6 - heartbeat checker
    public void init() {
        es.scheduleAtFixedRate(this::heartBeatCheck, 0, heartBeatFrequency, TimeUnit.SECONDS);
    }

    // Step 6 - close heartbeat checker
    public void close() {
        es.shutdown();
    }

    // method used for testing
    public void getProviders() {
        poolLock.lock();
        for (ProviderUnit providerUnit : providerPool) {
            System.out.println(providerUnit);
        }
        poolLock.unlock();
    }

    // method used for testing
    public void changeMethod(RetrieveMethod method) {
        this.retrieveMethod = method;
    }

}
