package coordinator;

public class Main {
    public static void main(String[] args) {

        Coordinator coordinator = new Coordinator(5000);

        new Thread(coordinator).start();
    }

}
