package com.learning;

public class Main {
    public static void main(String[] args) throws Exception {
        boolean onS3 = args[0].equals("s3");

        if (onS3 && args.length == 3) {
            System.exit(WordCountRunner.run(args[1], args[2], true));
        } else if (!onS3 && args.length == 2) {
            System.exit(WordCountRunner.run(args[0], args[1],false));
        } else {
            System.out.println("Error! False list of arguments!");
            System.exit(2);
        }
    }
}
