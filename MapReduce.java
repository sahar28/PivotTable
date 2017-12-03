package mapreduce;




import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

public class MapReduce {


	static String csvFile ="C:\\Users\\sahar\\OneDrive\\Documents\\Teste_MapReduce.csv";
	static String ligne = "";
	static String separateurCsv = ";";
	static String [] test;
	// Création du HashMap avec le concept des keys/values
	static HashMap<Integer, String> newmap = new HashMap<Integer, String>();
	static String[] testMap = new String[9];
	static String[] finalTab = new String[9];
	static int a=0;
	
	public static void main (String[] args) {
		int i = 0;
		try (BufferedReader br = new BufferedReader (new FileReader(csvFile))){
			@SuppressWarnings("unchecked")
			ArrayList <String[]> tabMap = new ArrayList<>();
			while ((ligne = br.readLine()) != null) {
				HashMap<Integer, String>  toReduce = new HashMap<Integer, String>();
				finalTab = map(i,ligne.toString());
				i++;
			}
			for (String values: finalTab){
				System.out.println(values);
			}
			System.out.print(finalTab[0]);
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	// Fonction Map
	static String[] map (int key, String values){
		String [] tableauMap = ligne.split(separateurCsv);
		finalTab = reduce(a, tableauMap);
		
		return finalTab;
		
	}
	
	// Fonction Reduce
	static String[] reduce (int a, String [] tableauMap) {
		for (String maps : tableauMap) {
			if (testMap[a] != null) {
				testMap[a] =  testMap[a] + ';' + maps;
			}else {
				testMap[a] =  maps;
			}
			a++;
		}a=0;
		return testMap;
	}
}