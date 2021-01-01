package com.ngt.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.File;
import java.io.IOException;
/**
 * @author ngt
 * @create 2020-12-23 22:06
 */
public class JsonToCSV {
    /**
     *
     * @param jsonPath
     * @param csvPath
     * @throws IOException
     */
    public static void jsonToCSV(String jsonPath, String csvPath) throws IOException {
        JsonNode jsonTree = new ObjectMapper().readTree(new File(jsonPath));
        CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder();
        JsonNode firstObject = jsonTree.elements().next();
        firstObject.fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
        CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();

        CsvMapper csvMapper = new CsvMapper();
        csvMapper.writerFor(JsonNode.class)
                .with(csvSchema)
                .writeValue(new File(csvPath), jsonTree);
    }
}
