/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.ewh.data_structures;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.ujmp.core.intmatrix.impl.DefaultSparseIntMatrix;
import org.ujmp.core.io.ImportMatrixSPARSECSV;

import ch.epfl.data.squall.ewh.visualize.VisualizerInterface;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.DeepCopy;
import ch.epfl.data.squall.utilities.MyUtilities;
import ch.epfl.data.squall.utilities.SystemParameters;

// Join Attribute Type
public class UJMPAdapterIntMatrix<JAT extends Comparable<JAT>> extends
	JoinMatrix<JAT> {
    private static Logger LOG = Logger.getLogger(UJMPAdapterIntMatrix.class);

    private int _capacity = -1;

    private Map _map;

    public UJMPAdapterIntMatrix(int xSize, int ySize, Map map,
	    ComparisonPredicate<JAT> cp, Type<JAT> wrapper) {
	this(xSize, ySize, map);
	_cp = cp;
	_wrapper = (NumericType) wrapper;
    }

    public UJMPAdapterIntMatrix(int xSize, int ySize, Map map) {
	this(xSize, ySize);
	_map = map;
	_matrixPath = SystemParameters.getString(map, "DIP_MATRIX_ROOT") + "/";
	_matrixName = MyUtilities.getQueryID(map); // SystemParameters.getString(map,
						   // "DIP_QUERY_NAME");
    }

    // not used anywhere
    public UJMPAdapterIntMatrix(int numNonZeros, int xSize, int ySize) {
	_capacity = numNonZeros;
	_ujmpMatrix = new DefaultSparseIntMatrix(_capacity, new long[] { xSize,
		ySize }); // The first argument is MAX_SIZE
    }

    public UJMPAdapterIntMatrix(int xSize, int ySize) {
	_capacity = SystemParameters.MATRIX_CAPACITY_MULTIPLIER
		* (xSize + ySize);
	_ujmpMatrix = new DefaultSparseIntMatrix(_capacity, new long[] { xSize,
		ySize }); // The first argument is MAX_SIZE

	/*
	 * return new DefaultDenseBooleanMatrix2D(joinMatrix);
	 * DefaultBooleanMatrix2DFactory, DenseFileMatrix int rows = 5; int cols
	 * = 5; m1 = new DefaultDenseBooleanMatrix2D(rows, cols);
	 */

	/*
	 * m1.setAsBoolean(true, 0, 2); // select a small portion of the matrix
	 * // and fill it with random values m2 = m1 . select ( Ret . LINK ,
	 * " 1000 -5000;1000 -3000 " ); m2 . rand ( Ret . ORIG ); // select
	 * another submatrix and subtract 2.0 m3 = m1 . select ( Ret . LINK ,
	 * " 1000 -2000;1000 -2000 " ); m3 . minus ( Ret . ORIG , false , 2.0);
	 */
    }

    public UJMPAdapterIntMatrix(String matrixPath, String matrixName) {
	_matrixPath = matrixPath;
	_matrixName = matrixName;

	try {
	    String path = _matrixPath + "/" + _matrixName;

	    // UJMP bug: The matrix is missing one element from X dimension
	    _ujmpMatrix = new DefaultSparseIntMatrix(
		    ImportMatrixSPARSECSV.fromFile(new File(path)));
	} catch (Exception exc) {
	    LOG.info(MyUtilities.getStackTrace(exc));
	}
	// PLT format is the only thing we could use for saving graphs for the
	// papers
    }

    private UJMPAdapterIntMatrix() {
    }

    @Override
    public UJMPAdapterIntMatrix<JAT> getDeepCopy() {
	if (_capacity == -1) {
	    // read from file, cannot make deep copy
	    throw new RuntimeException(
		    "Cannot make a deep copy of UJMPAdapterIntMatrix which created from a file!");
	}
	UJMPAdapterIntMatrix<JAT> copy = new UJMPAdapterIntMatrix<JAT>();
	copy._capacity = _capacity;
	copy._map = (Map) DeepCopy.copy(_map);
	copy._matrixPath = _matrixPath;
	copy._matrixName = _matrixName;
	copy._ujmpMatrix = new DefaultSparseIntMatrix(_ujmpMatrix, _capacity);
	copy._regions = (List<Region>) DeepCopy.copy(_regions);
	copy._joinAttributeX = (List<JAT>) DeepCopy.copy(_joinAttributeX);
	copy._joinAttributeY = (List<JAT>) DeepCopy.copy(_joinAttributeY);
	copy._freqX = (Map<JAT, Integer>) DeepCopy.copy(_freqX);
	copy._freqY = (Map<JAT, Integer>) DeepCopy.copy(_freqY);
	copy._keyXFirstPos = (Map<JAT, Integer>) DeepCopy.copy(_keyXFirstPos);
	copy._keyYFirstPos = (Map<JAT, Integer>) DeepCopy.copy(_keyYFirstPos);
	copy._wrapper = (NumericType) DeepCopy.copy(_wrapper);
	copy._cp = (ComparisonPredicate) DeepCopy.copy(_cp);

	return copy;
    }

    @Override
    public Map getConfiguration() {
	return _map;
    }

    @Override
    public long getCapacity() {
	return _capacity;
    }

    @Override
    public void visualize(VisualizerInterface visualizer) {
	visualizer.visualize(this);
    }

    @Override
    public void setElement(int value, int x, int y) {
	_ujmpMatrix.setAsInt(value, x, y);

	/*
	 * Alternatives: _ujmpMatrix.setAsObject(null, x, y);
	 * _ujmpMatrix.delete(null, new int[]{x, y});
	 */
    }

    @Override
    public void increment(int x, int y) {
	increase(1, x, y);
    }

    @Override
    public void increase(int delta, int x, int y) {
	int oldValue = getElement(x, y);
	int newValue = oldValue + delta;
	setElement(newValue, x, y);
    }

    @Override
    public void setMinPositiveValue(int x, int y) {
	setElement(1, x, y);
    }

    @Override
    public int getMinPositiveValue() {
	return 1;
    }

    @Override
    public int getElement(int x, int y) {
	return _ujmpMatrix.getAsInt(x, y);
    }

    @Override
    public boolean isEmpty(int x, int y) {
	return getElement(x, y) == 0;
    }

    public static void main(String[] args) {
	UJMPAdapterIntMatrix<Integer> joinMatrix = new UJMPAdapterIntMatrix<Integer>(
		100, 100);
	joinMatrix.setElement(4, 2, 2);
	joinMatrix.increment(2, 2);
	joinMatrix.setElement(0, 5, 3);

	Iterator<long[]> coordinates = joinMatrix
		.getNonEmptyCoordinatesIterator();
	while (coordinates.hasNext()) {
	    long[] coordinate = coordinates.next();
	    System.out.println("["
		    + coordinate[0]
		    + ", "
		    + coordinate[1]
		    + "] = "
		    + joinMatrix.getElement((int) coordinate[0],
			    (int) coordinate[1]));
	}
    }
}