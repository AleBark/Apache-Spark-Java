public class test {

    public static void main(String[] args) {
        String num = "5693435208";

        try {
            Double.parseDouble(num);
        }catch (Exception e){
            System.out.println(e);
        }

    }
}
