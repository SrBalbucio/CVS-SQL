package org.balbucio;

import balbucio.sqlapi.model.ResultValue;
import balbucio.sqlapi.sqlite.HikariSQLiteInstance;
import balbucio.sqlapi.sqlite.SQLiteInstance;
import balbucio.sqlapi.sqlite.SqliteConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        new Main();
    }

    public Main(){
        SqliteConfig config = new SqliteConfig(new File("database.db"));
        config.setMaxRows(Integer.MAX_VALUE);
        config.setQueryTimeout(Integer.MAX_VALUE);
        config.createFile();
        SQLiteInstance sqLiteInstance = new SQLiteInstance(config);
        if(!sqLiteInstance.tableExists("curso")) {
            sqLiteInstance.createTable("curso", "aluno VARCHAR(255), curso VARCHAR(255), valor DECIMAL(10.2)");
            sqLiteInstance.insert("aluno, curso, valor", "'Marcio', 'CyberSeguança', '2400.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'João', 'TI', '2040.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Tawan', 'Medicina', '2300.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Luiz', 'PowerBI', '2040.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Gustavo', 'Arquitetura', '2000.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Pedro', 'Agronomia', '1200.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Mulina', 'CyberSeguança', '9000.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Leandro', 'Agronegocio', '7000.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Cesar', 'Engenharia', '4000.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Laura', 'Fisioterapia', '32000.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Claudio', 'Administração', '23000.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Breno', 'CyberSeguança', '24000.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Bruno', 'Excel', '2000.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Alberto', 'CyberSeguança', '20005.0'", "curso");
            sqLiteInstance.insert("aluno, curso, valor", "'Cleito', 'CyberSeguança', '20050.0'", "curso");
        }
        File desc = new File("decrescente.csv");
        File acres = new File("crescente.csv");
        File regs = new File("registro.csv");

        String[] header = new String[]{ "aluno", "curso", "valor"};
        try {
            write(desc, header, sqLiteInstance.getPreparedStatement("SELECT * FROM curso ORDER BY aluno DESC;").executeQuery());
            write(acres, header, sqLiteInstance.getPreparedStatement("SELECT * FROM curso ORDER BY aluno;").executeQuery());
            write(regs, header, sqLiteInstance.getPreparedStatement("SELECT * FROM curso;").executeQuery());
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void write(File file, String[] header, ResultSet values) throws IOException, SQLException {
        if(file.exists()){
            file.delete();
        }
        FileWriter writer = new FileWriter(file);

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setSkipHeaderRecord(true)
                .setHeader(header)
                .build();

        try{
            final CSVPrinter printer = new CSVPrinter(writer, csvFormat);
            while (values.next()) {
                printer.printRecord(values.getString("aluno"), values.getString("curso"), values.getBigDecimal("valor"));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        writer.flush();
    }
}