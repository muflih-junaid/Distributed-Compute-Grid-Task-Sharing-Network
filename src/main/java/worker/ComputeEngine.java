package worker;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public final class ComputeEngine {

    private ComputeEngine() {
    }
    
    public static List<List<BigDecimal>> multiply(List<?> matrixA, List<?> matrixB, List<?> rowRange) {
        if (matrixA == null || matrixB == null || rowRange == null
                || rowRange.size() != 2 || matrixA.isEmpty() || matrixB.isEmpty()) {
            throw new IllegalArgumentException("Matrices and a two-value rowRange are required");
        }

        int start = requireInteger(rowRange.get(0), "rowRange");
        int end = requireInteger(rowRange.get(1), "rowRange");
        if (start < 0 || end < start || (long) end - start + 1 != matrixA.size()) {
            throw new IllegalArgumentException("rowRange must describe the supplied rows of matrixA");
        }

        List<List<BigDecimal>> rowsA = parseMatrix(matrixA, "matrixA");
        List<List<BigDecimal>> rowsB = parseMatrix(matrixB, "matrixB");
        int sharedDimension = rowsA.get(0).size();
        int outputColumns = rowsB.get(0).size();
        if (sharedDimension == 0 || outputColumns == 0) {
            throw new IllegalArgumentException("Matrix rows cannot be empty");
        }
        if (rowsB.size() != sharedDimension) {
            throw new IllegalArgumentException("matrixA columns must match matrixB rows");
        }

        List<List<BigDecimal>> result = new ArrayList<>();
        for (List<BigDecimal> rowA : rowsA) {
            List<BigDecimal> outputRow = new ArrayList<>();
            for (int column = 0; column < outputColumns; column++) {
                BigDecimal sum = BigDecimal.ZERO;
                for (int index = 0; index < sharedDimension; index++) {
                    sum = sum.add(rowA.get(index).multiply(rowsB.get(index).get(column)));
                }
                outputRow.add(sum);
            }
            result.add(outputRow);
        }

        return result;
    }

    private static List<List<BigDecimal>> parseMatrix(List<?> matrix, String matrixName) {
        List<List<BigDecimal>> parsed = new ArrayList<>();
        int columnCount = -1;
        for (Object rowValue : matrix) {
            if (!(rowValue instanceof List<?>)) {
                throw new IllegalArgumentException(matrixName + " must contain only array rows");
            }
            List<?> row = (List<?>) rowValue;
            if (columnCount == -1) {
                columnCount = row.size();
            } else if (row.size() != columnCount) {
                throw new IllegalArgumentException(matrixName + " must be rectangular");
            }

            List<BigDecimal> parsedRow = new ArrayList<>();
            for (Object value : row) {
                parsedRow.add(requireNumber(value, matrixName));
            }
            parsed.add(parsedRow);
        }
        return parsed;
    }

    private static int requireInteger(Object value, String fieldName) {
        try {
            return requireNumber(value, fieldName).intValueExact();
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException(fieldName + " values must be integers in range", exception);
        }
    }

    private static BigDecimal requireNumber(Object value, String fieldName) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException(fieldName + " values must be numbers");
        }
        try {
            return new BigDecimal(value.toString());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(fieldName + " contains an invalid number", exception);
        }
    }
}
