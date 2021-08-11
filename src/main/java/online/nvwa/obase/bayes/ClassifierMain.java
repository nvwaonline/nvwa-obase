package online.nvwa.obase.bayes;

public class ClassifierMain {
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub  
//        DataPreProcess DataPP = new DataPreProcess();  
//        NaiveBayesianClassifier nbClassifier = new NaiveBayesianClassifier();  
        KNNClassifier knnClassifier = new KNNClassifier();  
//        DataPP.BPPMain(args);  
//        nbClassifier.NaiveBayesianClassifierMain(args);  
        knnClassifier.KNNClassifierMain(args);  
    }  
}
