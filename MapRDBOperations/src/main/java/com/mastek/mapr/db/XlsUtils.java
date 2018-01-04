package com.mastek.mapr.db;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

public class XlsUtils {
	public static void readExcel(Properties props){
		try {
            FileInputStream excelFile = new FileInputStream(new File(props.getProperty("excelFileName")));
            Workbook workbook = new XSSFWorkbook(excelFile);
            Sheet datatypeSheet = workbook.getSheetAt(0);
            Iterator<Row> iterator = datatypeSheet.iterator();

            while (iterator.hasNext()) {

                Row currentRow = iterator.next();
                try	{
                	String valueToUpdate = "";
                	String startKey = "";
                	String endKey = "";
                	
                	if (currentRow.getCell(0) != null && currentRow.getCell(0).getCellTypeEnum() != CellType.BLANK) {
                		valueToUpdate = currentRow.getCell(0).getStringCellValue();
                	}
                	
                	if (currentRow.getCell(1) != null && currentRow.getCell(1).getCellTypeEnum() != CellType.BLANK) {
                		startKey = currentRow.getCell(1).getStringCellValue();
                	} 
                	
                	if (currentRow.getCell(2) != null && currentRow.getCell(2).getCellTypeEnum() != CellType.BLANK) {
                		endKey = currentRow.getCell(2).getStringCellValue();
                	}
                	
                	System.out.println("Reading from excel - " + valueToUpdate + " " + startKey + " " + endKey);
                	
                	if(valueToUpdate != ""){
                		MapRDBUtils.scanDB(valueToUpdate, startKey, endKey);
                	}
                	
                	if(endKey != ""){
                		 MapRDBUtils.putRecord(endKey, props.getProperty("columnFamily"), props.getProperty("columnQualifier"), valueToUpdate);
                	}
                	
                	System.out.println();
                } catch(Exception e){
                	e.printStackTrace();
                }
//                Iterator<Cell> cellIterator = currentRow.iterator();
//                while (cellIterator.hasNext()) {
//                    Cell currentCell = cellIterator.next();
//                    //getCellTypeEnum shown as deprecated for version 3.15
//                    //getCellTypeEnum ill be renamed to getCellType starting from version 4.0
//                    if (currentCell.getCellTypeEnum() == CellType.STRING) {
//                        System.out.print(currentCell.getStringCellValue() + "--");
//                    } else if (currentCell.getCellTypeEnum() == CellType.NUMERIC) {
//                        System.out.print(currentCell.getNumericCellValue() + "--");
//                    }
//                }
//                System.out.println();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
}