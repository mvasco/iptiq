import java.util.concurrent.atomic.AtomicInteger;

public class Provider {

    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(1001);

    private final int uid;

    private Provider(int id) {
        uid = id;
    }

    public static Provider newProvider() {
        return new Provider(ID_GENERATOR.getAndIncrement());
    }

    public String get() {
        return String.valueOf(uid);
    }

    public int getId() {
        return uid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return this.uid == ((Provider)o).getId();
    }

    @Override
    public int hashCode() {
        return uid;
    }

    public boolean check() {
        return true;
    }

}
