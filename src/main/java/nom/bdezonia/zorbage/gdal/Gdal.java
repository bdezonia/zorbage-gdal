/*
 * zorbage-gdal: code for using the gdal data file library to open files into zorbage data structures for further processing
 *
 * Copyright (C) 2020-2022 Barry DeZonia
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package nom.bdezonia.zorbage.gdal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Dimension;
import org.gdal.gdal.Group;
import org.gdal.gdal.MDArray;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;

import nom.bdezonia.zorbage.algebra.Algebra;
import nom.bdezonia.zorbage.algebra.Allocatable;
import nom.bdezonia.zorbage.algebra.G;
import nom.bdezonia.zorbage.algebra.SetFromBytes;
import nom.bdezonia.zorbage.algebra.SetFromFloats;
import nom.bdezonia.zorbage.coordinates.CoordinateSpace;
import nom.bdezonia.zorbage.coordinates.LinearNdCoordinateSpace;
import nom.bdezonia.zorbage.data.DimensionedDataSource;
import nom.bdezonia.zorbage.data.DimensionedStorage;
import nom.bdezonia.zorbage.dataview.PlaneView;
import nom.bdezonia.zorbage.misc.DataBundle;
import nom.bdezonia.zorbage.procedure.Procedure2;
import nom.bdezonia.zorbage.procedure.Procedure3;
import nom.bdezonia.zorbage.sampling.IntegerIndex;
import nom.bdezonia.zorbage.sampling.SamplingCartesianIntegerGrid;
import nom.bdezonia.zorbage.sampling.SamplingIterator;
import nom.bdezonia.zorbage.type.complex.float32.ComplexFloat32Member;
import nom.bdezonia.zorbage.type.complex.float64.ComplexFloat64Member;
import nom.bdezonia.zorbage.type.gaussian.int16.GaussianInt16Member;
import nom.bdezonia.zorbage.type.gaussian.int32.GaussianInt32Member;
import nom.bdezonia.zorbage.type.integer.int16.SignedInt16Member;
import nom.bdezonia.zorbage.type.integer.int16.UnsignedInt16Member;
import nom.bdezonia.zorbage.type.integer.int32.SignedInt32Member;
import nom.bdezonia.zorbage.type.integer.int32.UnsignedInt32Member;
import nom.bdezonia.zorbage.type.integer.int64.SignedInt64Member;
import nom.bdezonia.zorbage.type.integer.int64.UnsignedInt64Member;
import nom.bdezonia.zorbage.type.integer.int8.SignedInt8Member;
import nom.bdezonia.zorbage.type.integer.int8.UnsignedInt8Member;
import nom.bdezonia.zorbage.type.real.float32.Float32Member;
import nom.bdezonia.zorbage.type.real.float64.Float64Member;

/**
 * 
 * @author Barry DeZonia
 *
 */
public class Gdal {

	private static final int MAXCOLS = 512;
	
	/**
	 * This must be called once at startup by users of this gdal interface package
	 */
	public static int
	
		init()
	{

		try {
		
			String cmd = "gdalinfo --version";
			
			Runtime run = Runtime.getRuntime();
			
			Process pr = run.exec(cmd);
			
			pr.waitFor();
			
			BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
			
			String line = "";
			
			boolean found = false;
			
			while ((line=buf.readLine())!=null) {
			
				if (line.contains("GDAL"))
					found = true;
			}
			
			if (!found) {
			
				return 1;
			}
			
		} catch (IOException e) {
			
			return 2;
			
		} catch (InterruptedException e) {
			
			return 3;
		}
		
		gdal.AllRegister();
		
		return 0;
	}
	
	/**
	 * 
	 * @param filename
	 */
	@SuppressWarnings("unchecked")
	public static DataBundle
	
		loadAllDatasets(String filename)
	{
		final DataBundle outputs = new DataBundle();
		
		Dataset ds = gdal.OpenEx(filename, gdalconst.OF_MULTIDIM_RASTER);

		final Group group;
		
		if (ds == null) {
			
			group = null;
		}
		else {
			
			group = ds.GetRootGroup();
		}


		// let's deal with a multi dim dataset if we can
		
		Vector<String> mdArrayNames =  new Vector<String>();
		
		if (group != null) {
			
			//@SuppressWarnings("unchecked")
			mdArrayNames = (Vector<String>) group.GetMDArrayNames();

			System.out.println("Found "+mdArrayNames.size()+" mdarrays");
		}
		
		if (mdArrayNames.size() > 0) {
			
			for (int i = 0; i < mdArrayNames.size(); i++) {
		
				String name = mdArrayNames.get(i);
				
				System.out.println("array " + i + " is called " + name);
				
				MDArray data = group.OpenMDArray(name);
				
				long nDim = data.GetDimensionCount();
				
				if (nDim > Integer.MAX_VALUE) {
					
					throw new IllegalArgumentException("cannot handle this many dimensions!");
				}

				Double[] scales = new Double[(int) nDim];
				
				data.GetScale(scales);

				Double[] offsets = new Double[(int) nDim];
				
				data.GetOffset(offsets);
				
				for (int k = 0; k < nDim; k++) {
					System.out.println("  axis "+k+" dim "+data.GetDimension(k).GetSize()+" scale "+scales[k]+" offset "+offsets[k]);
				}

				System.out.println("  name "+data.GetName());
				
				System.out.println("  full name "+data.GetFullName());
				
				System.out.println("  unit "+data.GetUnit());
				
				System.out.println("  data type "+gdal.GetDataTypeName(data.GetDataType().GetNumericDataType()));
				
				System.out.println("  and has " + data.GetDimensionCount() + " dimensions.");
				
				System.out.println("  any structural info follows here:");
				
				Hashtable<String,?> ht = data.GetStructuralInfo();
				
				Enumeration<String> keys = ht.keys();
				
				Iterator<String> iter = keys.asIterator();
				
				while (iter.hasNext()) {
					
					String key = iter.next();
					
					System.out.println("    key "+key+" value "+ht.get(key));
				}
				
				int type = data.GetDataType().GetNumericDataType();
				
				if (type == gdalconst.GDT_Byte) {

					outputs.mergeUInt8(readMDArrayUByteData(data, G.UINT8.construct()));
				}
				else if (type == gdalconst.GDT_Int8) {

					outputs.mergeInt8(readMDArrayByteData(data, G.INT8.construct()));
				}
				else if (type == gdalconst.GDT_UInt16) {
					
					outputs.mergeUInt16(readMDArrayUShortData(data, G.UINT16.construct()));
				}
				else if (type == gdalconst.GDT_Int16) {
					
					outputs.mergeInt16(readMDArrayShortData(data, G.INT16.construct()));
				}
				else if (type == gdalconst.GDT_UInt32) {
					
					outputs.mergeUInt32(readMDArrayUIntData(data, G.UINT32.construct()));
				}
				else if (type == gdalconst.GDT_Int32) {
					
					outputs.mergeInt32(readMDArrayIntData(data, G.INT32.construct()));
				}
				else if (type == gdalconst.GDT_UInt64) {
					
					outputs.mergeUInt64(readMDArrayULongData(data, G.UINT64.construct()));
				}
				else if (type == gdalconst.GDT_Int64) {
					
					outputs.mergeInt64(readMDArrayLongData(data, G.INT64.construct()));
				}
				else if (type == gdalconst.GDT_Float32) {
					
					outputs.mergeFlt32(readMDArrayFloatData(data, G.FLT.construct()));
				}
				else if (type == gdalconst.GDT_Float64) {
					
					outputs.mergeFlt64(readMDArrayDoubleData(data, G.DBL.construct()));
				}
				else if (type == gdalconst.GDT_CInt16) {
					
					outputs.mergeGaussianInt16(readMDArrayGaussianShortData(data, G.GAUSS16.construct()));
				}
				else if (type == gdalconst.GDT_CInt32) {
					
					outputs.mergeGaussianInt32(readMDArrayGaussianIntData(data, G.GAUSS32.construct()));
				}
				else if (type == gdalconst.GDT_CFloat32) {
					
					outputs.mergeComplexFlt32(readMDArrayComplexFloatData(data, G.CFLT.construct()));
				}
				else if (type == gdalconst.GDT_CFloat64) {
					
					outputs.mergeComplexFlt64(readMDArrayComplexDoubleData(data, G.CDBL.construct()));
				}
				else if (type != -1) {
				
					System.out.println("Ignoring unknown data type "+gdal.GetDataTypeName(type));
				}
			}
		}
		else {
		
			// old fashioned 1, 2, or 3 dim image
	
			ds = gdal.OpenEx(filename);
			
			Vector<String> subdatasetInfo = (Vector<String>) ds.GetMetadata_List("SUBDATASETS");
			
			int counter = 1;
			
			for (String entry : subdatasetInfo) {
			
				String namePrefix = "SUBDATASET_" + counter + "_NAME=";
				
				if (entry.startsWith(namePrefix)) {
				
					String[] pair = entry.split("=");
					
					if (pair.length != 2)
						throw new IllegalArgumentException("gdal metadata: too many equal signs in internal filename");
					
					DataBundle lowerbundle = loadAllDatasets(pair[1]);
					
					outputs.mergeAll(lowerbundle);
					
					counter++;
				}
			}
	
			int type = -1;
		
			int xSize = ds.GetRasterXSize();
			
			int ySize = ds.getRasterYSize();
			
			for (int i = 1; i <= ds.GetRasterCount(); i++) {
			
				Band band = ds.GetRasterBand(i);
				
				if (type == -1) {
	
					type = band.GetRasterDataType();
				}
				
				if (band.GetRasterDataType() != type) {
					
					throw new IllegalArgumentException("data has multiple different band types!");
				}
				
				if ((band.GetXSize() != xSize) || (band.GetYSize() != ySize)) {
					
					throw new IllegalArgumentException("data has multiple band resolutions!");
				}
			}
			
			if (type == gdalconst.GDT_Byte) {
	
				outputs.mergeUInt8(readBandedUByteData(ds, G.UINT8.construct()));
			}
			else if (type == gdalconst.GDT_Int8) {
	
				outputs.mergeInt8(readBandedByteData(ds, G.INT8.construct()));
			}
			else if (type == gdalconst.GDT_UInt16) {
				
				outputs.mergeUInt16(readBandedUShortData(ds, G.UINT16.construct()));
			}
			else if (type == gdalconst.GDT_Int16) {
				
				outputs.mergeInt16(readBandedShortData(ds, G.INT16.construct()));
			}
			else if (type == gdalconst.GDT_UInt32) {
				
				outputs.mergeUInt32(readBandedUIntData(ds, G.UINT32.construct()));
			}
			else if (type == gdalconst.GDT_Int32) {
				
				outputs.mergeInt32(readBandedIntData(ds, G.INT32.construct()));
			}
			else if (type == gdalconst.GDT_UInt64) {
				
				outputs.mergeUInt64(readBandedUIntData(ds, G.UINT64.construct()));
			}
			else if (type == gdalconst.GDT_Int64) {
				
				outputs.mergeInt64(readBandedIntData(ds, G.INT64.construct()));
			}
			else if (type == gdalconst.GDT_Float32) {
				
				outputs.mergeFlt32(readBandedFloatData(ds, G.FLT.construct()));
			}
			else if (type == gdalconst.GDT_Float64) {
				
				outputs.mergeFlt64(readBandedDoubleData(ds, G.DBL.construct()));
			}
			else if (type == gdalconst.GDT_CInt16) {
				
				outputs.mergeGaussianInt16(readBandedGaussianShortData(ds, G.GAUSS16.construct()));
			}
			else if (type == gdalconst.GDT_CInt32) {
				
				outputs.mergeGaussianInt32(readBandedGaussianIntData(ds, G.GAUSS32.construct()));
			}
			else if (type == gdalconst.GDT_CFloat32) {
				
				outputs.mergeComplexFlt32(readBandedComplexFloatData(ds, G.CFLT.construct()));
			}
			else if (type == gdalconst.GDT_CFloat64) {
				
				outputs.mergeComplexFlt64(readBandedComplexDoubleData(ds, G.CDBL.construct()));
			}
			else if (type != -1) {
			
				System.out.println("Ignoring unknown data type "+gdal.GetDataTypeName(type));
			}
		}

		return outputs;
	}
	
	private static <U extends Allocatable<U>> DimensionedDataSource<U>
	
		readBandedData(Dataset ds, U var, Procedure2<BandBuffer, U> proc)
	{
		int numPlanes = ds.getRasterCount();
		
		long[] dims;
		
		if (numPlanes == 1) {
		
			dims = new long[] {ds.getRasterXSize(), ds.GetRasterYSize()};
		}
		else {
			
			dims = new long[] {ds.getRasterXSize(), ds.GetRasterYSize(), numPlanes};
		}
		
		DimensionedDataSource<U> data = DimensionedStorage.allocate(var, dims);
		
		PlaneView<U> planes = new PlaneView<>(data, 0, 1);
		
		int numD = data.numDimensions();
		
		for (int i = 0; i < numPlanes; i++) {
			
			Band band = ds.GetRasterBand(i+1);
			
			if (i == 0) {
			
				data.setValueUnit(band.GetUnitType());
			
				BigDecimal[] scales = new BigDecimal[numD];
				
				BigDecimal[] offsets = new BigDecimal[numD];
				
				Double[] scls = new Double[numD];
				
				Double[] offs = new Double[numD];
				
				band.GetScale(scls);
				
				band.GetOffset(offs);
				
				boolean definitionsOkay = true;
				
				for (int d = 0; d < numD; d++) {
				
					if (scls[d] == null) definitionsOkay = false;
					
					if (offs[d] == null) definitionsOkay = false;
				}
				
				if (definitionsOkay) {
				
					for (int d = 0; d < numD; d++) {
					
						scales[d] = BigDecimal.valueOf(scls[d]);
						
						offsets[d] = BigDecimal.valueOf(offs[d]);
					}
					
					CoordinateSpace cspace = new LinearNdCoordinateSpace(scales, offsets);
					
					data.setCoordinateSpace(cspace);
				}
			}
			
			data.metadata().putString("band-"+i+"-description", band.GetDescription());
			
			data.metadata().putString("band-"+i+"-units", band.GetUnitType());
			
			@SuppressWarnings("unchecked")
			Hashtable<String,String> table = (Hashtable<String,String>) band.GetMetadata_Dict();
			
			for (String key : table.keySet()) {
			
				String value = table.get(key);
				
				if (key != null && key.length() > 0) {
				
					if (value != null && value.length() > 0) {
					
						data.metadata().putString("band-"+i+"-"+key, value);
					}
				}
			}

			if (data.numDimensions() > 2) {

				planes.setPositionValue(0, i);
			}
			
			for (int y = 0; y < ds.GetRasterYSize(); y++) {
		
				BandBuffer bandBuf = new BandBuffer(band, y, ds.getRasterXSize());
				
				for (int x = 0; x < ds.GetRasterXSize(); x++) {
				
					proc.call(bandBuf, var);
					
					planes.set(x, y, var);
				}				
			}
		}
		
		return data;
	}

	private static long[] ones(long count) {
		
		if (count > Integer.MAX_VALUE)
			throw new IllegalArgumentException("too many dimensions!");
		
		long[] vals = new long[(int) count];
		for (int i = 0; i < count; i++) {
			vals[i] = 1;
		}
		return vals;
	}

	interface Buffer<U> {
		
		void readData(MDArray data, long[] gdalIdx, long[] gdalShape);
		int numInBuffer();
		void getVal(int i, U val);
	}
	
	private static <U extends SetFromBytes>
		class ByteBuffer<U> implements Buffer<U>
	{
		byte[] buffer;
		byte[] miniBuff;
		int currIndex;
		int numInBuffer;
		int numComponents;
		
		ByteBuffer(int numComponenets) {
			
			this.numComponents = numComponenets;
			this.miniBuff = new byte[numComponents];
			this.buffer = new byte[MAXCOLS * numComponents];
			this.numInBuffer = -1;
			this.currIndex = -1;
		}
		
		@Override
		public void readData(MDArray data, long[] gdalIdx, long[] gdalShape) {
			
			data.Read(gdalIdx, gdalShape, buffer);
			
			currIndex = 0;
			
			numInBuffer = (int) gdalShape[gdalShape.length-1];
		}

		@Override
		public int numInBuffer() {

			return numInBuffer;
		}

		@Override
		public void getVal(int i, U val) {
			
			if (i < 0 || i >= numInBuffer)
				throw new IllegalArgumentException("buffer access out of bounds");
			
			for (int k = 0; k < numComponents; k++) {
				miniBuff[k] = buffer[numComponents*currIndex + k];
			}

			val.setFromBytes(miniBuff);
		}
		
	}
	
	private static <U extends SetFromShorts>
		class ShortBuffer<U> implements Buffer<U>
	{
		short[] buffer;
		short[] miniBuff;
		int currIndex;
		int numInBuffer;
		int numComponents;
		
		ByteBuffer(int numComponents) {
			
			this.numComponents = numComponents;
			this.miniBuff = new short[numComponents];
			this.buffer = new short[MAXCOLS * numComponents];
			this.numInBuffer = -1;
			this.currIndex = -1;
		}
		
		@Override
		public void readData(MDArray data, long[] gdalIdx, long[] gdalShape) {
			
			data.Read(gdalIdx, gdalShape, buffer);
			
			currIndex = 0;
			
			numInBuffer = (int) gdalShape[gdalShape.length-1];
		}

		@Override
		public int numInBuffer() {

			return numInBuffer;
		}

		@Override
		public void getVal(int i, U val) {
			
			if (i < 0 || i >= numInBuffer)
				throw new IllegalArgumentException("buffer access out of bounds");
			
			for (int k = 0; k < numComponents; k++) {
				miniBuff[k] = buffer[numComponents*currIndex + k];
			}

			val.setFromShorts(miniBuff);
		}
		
	}
	
	private static <U extends SetFromInts>
		class IntBuffer<U> implements Buffer<U>
	{
		int[] buffer;
		int[] miniBuff;
		int currIndex;
		int numInBuffer;
		int numComponents;
		
		ByteBuffer(int numComponents) {
			
			this.numComponents = numComponents;
			this.miniBuff = new int[numComponents];
			this.buffer = new int[MAXCOLS * numComponents];
			this.numInBuffer = -1;
			this.currIndex = -1;
		}
		
		@Override
		public void readData(MDArray data, long[] gdalIdx, long[] gdalShape) {
			
			data.Read(gdalIdx, gdalShape, buffer);
			
			currIndex = 0;
			
			numInBuffer = (int) gdalShape[gdalShape.length-1];
		}

		@Override
		public int numInBuffer() {

			return numInBuffer;
		}

		@Override
		public void getVal(int i, U val) {
			
			if (i < 0 || i >= numInBuffer)
				throw new IllegalArgumentException("buffer access out of bounds");
			
			for (int k = 0; k < numComponents; k++) {
				miniBuff[k] = buffer[numComponents*currIndex + k];
			}

			val.setFromInts(miniBuff);
		}
		
	}
	
	private static <U extends SetFromLongs>
		class LongBuffer<U> implements Buffer<U>
	{
		long[] buffer;
		long[] miniBuff;
		int currIndex;
		int numInBuffer;
		int numComponents;
		
		LongBuffer(int numComponents) {
			
			this.numComponents = numComponents;
			this.miniBuff = new long[numComponents];
			this.buffer = new long[MAXCOLS * numComponents];
			this.numInBuffer = -1;
			this.currIndex = -1;
		}
		
		@Override
		public void readData(MDArray data, long[] gdalIdx, long[] gdalShape) {
			
			data.Read(gdalIdx, gdalShape, buffer);
			
			currIndex = 0;
			
			numInBuffer = (int) gdalShape[gdalShape.length-1];
		}

		@Override
		public int numInBuffer() {

			return numInBuffer;
		}

		@Override
		public void getVal(int i, U val) {
			
			if (i < 0 || i >= numInBuffer)
				throw new IllegalArgumentException("buffer access out of bounds");
			
			for (int k = 0; k < numComponents; k++) {
				miniBuff[k] = buffer[numComponents*currIndex + k];
			}

			val.setFromLongs(miniBuff);
		}
		
	}
	
	private static <U extends SetFromFloats>
		class FloatBuffer<U> implements Buffer<U>
	{
		float[] buffer;
		float[] miniBuff;
		int currIndex;
		int numInBuffer;
		int numComponents;
		
		FloatBuffer(int numComponents) {
			
			this.numComponents = numComponents;
			this.miniBuff = new float[numComponents];
			this.buffer = new float[MAXCOLS * numComponents];
			this.numInBuffer = -1;
			this.currIndex = -1;
		}
		
		@Override
		public void readData(MDArray data, long[] gdalIdx, long[] gdalShape) {
			
			data.Read(gdalIdx, gdalShape, buffer);
			
			currIndex = 0;
			
			numInBuffer = (int) gdalShape[gdalShape.length-1];
		}

		@Override
		public int numInBuffer() {

			return numInBuffer;
		}

		@Override
		public void getVal(int i, U val) {
			
			if (i < 0 || i >= numInBuffer)
				throw new IllegalArgumentException("buffer access out of bounds");
			
			for (int k = 0; k < numComponents; k++) {
				miniBuff[k] = buffer[numComponents*currIndex + k];
			}

			val.setFromFloats(miniBuff);
		}
		
	}
	
	private static <U extends SetFromDoubles>
		class DoubleBuffer<U> implements Buffer<U>
	{
		double[] buffer;
		double[] miniBuff;
		int currIndex;
		int numInBuffer;
		int numComponents;
		
		DoubleBuffer(int numComponents) {
			
			this.numComponents = numComponents;
			this.miniBuff = new double[numComponents];
			this.buffer = new double[MAXCOLS * numComponents];
			this.numInBuffer = -1;
			this.currIndex = -1;
		}
		
		@Override
		public void readData(MDArray data, long[] gdalIdx, long[] gdalShape) {
			
			data.Read(gdalIdx, gdalShape, buffer);
			
			currIndex = 0;
			
			numInBuffer = (int) gdalShape[gdalShape.length-1];
		}

		@Override
		public int numInBuffer() {

			return numInBuffer;
		}

		@Override
		public void getVal(int i, U val) {
			
			if (i < 0 || i >= numInBuffer)
				throw new IllegalArgumentException("buffer access out of bounds");
			
			for (int k = 0; k < numComponents; k++) {
				miniBuff[k] = buffer[numComponents*currIndex + k];
			}

			val.setFromDoubles(miniBuff);
		}
		
	}
	
	private static <T extends Algebra<T,U>, U extends Allocatable<U>>
	
		DimensionedDataSource<U>
	
			readMDArrayData(MDArray data, U type, Buffer<U> buffer)
	{
		long nd = data.GetDimensionCount();
		
		if (nd > Integer.MAX_VALUE)
			throw new IllegalArgumentException("data has too many dimensions");
		
		int numDims = (int) nd;
		
		long[] gdalDims = new long[numDims];
		
		long[] zorbDims = new long[numDims];
		
		for (int i = 0; i < numDims; i++) {
		
			gdalDims[i] = data.GetDimension(i).GetSize();
			
			zorbDims[numDims - 1 - i] = gdalDims[i];
		}
		
		long maxX = gdalDims[numDims-1];

		long maxY = 1;
		
		if (numDims > 1) maxY = gdalDims[numDims-2];

		U val = type.allocate();
		
		DimensionedDataSource<U> output =
				
				DimensionedStorage.allocate(type, zorbDims);

		long[] gdalIdx = new long[numDims];
		
		IntegerIndex zorbIdx = new IntegerIndex(numDims);
		
		long[] colDims = new long[numDims - 1];
		
		for (int i = 0; i < colDims.length; i++) {
	    	 
			colDims[i] = gdalDims[i];
		}
	    
		IntegerIndex colIdx = new IntegerIndex(numDims - 1);

		long[] gdalShape = new long[numDims];
		
		for (int i = 0; i < numDims; i++) {

			gdalShape[i] = 1;
		}

		SamplingIterator<IntegerIndex> iter = new SamplingCartesianIntegerGrid(colDims).iterator();
	    
		while (iter.hasNext()) {
			
			iter.next(colIdx);

			for (int i = 0; i < colIdx.numDimensions(); i++) {
				
				gdalIdx[i] = colIdx.get(i);
			}

			long left = 0;
		
			while (left < maxX) {
				
				long chunkSize = buffer.numInBuffer();
				
				if (left + chunkSize > maxX) {
				
					chunkSize = maxX - left;
				}
				
				gdalIdx[numDims-1] = left;

				gdalShape[numDims-1] = chunkSize;
				
				buffer.readData(data, gdalIdx, gdalShape);
				
				for (int i = 0; i < chunkSize; i++) {
					
					gdalIdx[numDims-1] = left + i;
					
					buffer.getVal(i, val);
					
					for (int k = 0; k < numDims; k++) {

						// is this the Y dim?
						
						if (k == numDims - 2) {
						
							// flip it
							zorbIdx.set(numDims - 1 - k, maxY - 1 - gdalIdx[k]);
						}
						else {  // don't flip it
							
							zorbIdx.set(numDims - 1 - k, gdalIdx[k]); 
						}
					}
				
					output.set(zorbIdx, val);
				}
	
				left += chunkSize;
			}
		}
		
		output.setName(data.GetName());
		output.setSource(data.GetFullName());
		output.setValueUnit(data.GetUnit());
		output.setValueType("unknown type");

		Double[] scales = new Double[numDims];
		Double[] offsets = new Double[numDims];
		
		data.GetScale(scales);
		data.GetOffset(offsets);
		
		BigDecimal[] bdScales = new BigDecimal[numDims];
		BigDecimal[] bdOffsets = new BigDecimal[numDims];

		for (int i = 0; i < numDims; i++) {
			
			Double scale = scales[i];
			Double offset = offsets[i];
			
			if (scale == null)
				bdScales[numDims - 1 - i] = BigDecimal.ONE;
			else
				bdScales[numDims - 1 - i] = BigDecimal.valueOf(scale);

			if (offset == null)
				bdOffsets[numDims - 1 - i] = BigDecimal.ZERO;
			else
				bdOffsets[numDims - 1 - i] = BigDecimal.valueOf(offset);
		}
		
		LinearNdCoordinateSpace space = new LinearNdCoordinateSpace(bdScales, bdOffsets);

		output.setCoordinateSpace(space);
		
		// TODO set more MetaData based upon gdal attributes?????
		
		return output;
	}

	private static DimensionedDataSource<UnsignedInt8Member>
	
		readMDArrayUByteData(MDArray data, UnsignedInt8Member type)
	{
		ByteBuffer<UnsignedInt8Member> buffer =
				new ByteBuffer<UnsignedInt8Member>(1);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<SignedInt8Member>
	
		readMDArrayByteData(MDArray data, SignedInt8Member type)
	{
		ByteBuffer<SignedInt8Member> buffer =
				new ByteBuffer<SignedInt8Member>(1);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<UnsignedInt16Member>
	
		readMDArrayUShortData(MDArray data, UnsignedInt16Member type)
	{
		ShortBuffer<UnsignedInt16Member> buffer =
				new ShortBuffer<UnsignedInt16Member>(1);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<SignedInt16Member>
	
		readMDArrayShortData(MDArray data, SignedInt16Member type)
	{
		ShortBuffer<SignedInt16Member> buffer =
				new ShortBuffer<SignedInt16Member>(1);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<UnsignedInt32Member>
	
		readMDArrayUIntData(MDArray data, UnsignedInt32Member type)
	{
		IntBuffer<UnsignedInt32Member> buffer =
				new IntBuffer<UnsignedInt32Member>(1);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<SignedInt32Member>
	
		readMDArrayIntData(MDArray data, SignedInt32Member type)
	{
		IntBuffer<SignedInt32Member> buffer =
				new IntBuffer<SignedInt32Member>(1);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<UnsignedInt64Member>
	
		readMDArrayULongData(MDArray data, UnsignedInt64Member var)
	{
		LongBuffer<UnsignedInt64Member> buffer =
				new LongBuffer<UnsignedInt64Member>(1);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<SignedInt64Member>
	
		readMDArrayLongData(MDArray data, SignedInt64Member var)
	{
		LongBuffer<SignedInt64Member> buffer =
				new LongBuffer<SignedInt64Member>(1);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<Float32Member>
	
		readMDArrayFloatData(MDArray data, Float32Member type)
	{
		FloatBuffer<Float32Member> buffer =
				new FloatBuffer<Float32Member>(1);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<Float64Member>
	
		readMDArrayDoubleData(MDArray data, Float64Member type)
	{
		DoubleBuffer<Float64Member> buffer =
				new DoubleBuffer<Float64Member>(1);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<ComplexFloat32Member>
	
		readMDArrayComplexFloatData(MDArray data, ComplexFloat32Member type)
	{
		FloatBuffer<ComplexFloat32Member> buffer =
				new FloatBuffer<ComplexFloat32Member>(2);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<ComplexFloat64Member>
	
		readMDArrayComplexDoubleData(MDArray data, ComplexFloat64Member type)
	{
		DoubleBuffer<ComplexFloat64Member> buffer =
				new DoubleBuffer<ComplexFloat64Member>(2);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<GaussianInt16Member>
	
		readMDArrayGaussianShortData(MDArray data, GaussianInt16Member type)
	{
		ShortBuffer<GaussianInt16Member> buffer =
				new ShortBuffer<GaussianInt16Member>(2);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<GaussianInt32Member>
	
		readMDArrayGaussianIntData(MDArray data, GaussianInt32Member var)
	{
		IntBuffer<GaussianInt32Member> buffer =
				new IntBuffer<GaussianInt32Member>(2);
		
		return readMDArrayData(data, type, buffer);
	}

	private static DimensionedDataSource<UnsignedInt8Member>
	
		readBandedUByteData(Dataset ds, UnsignedInt8Member var)
	{
		Procedure2<BandBuffer,UnsignedInt8Member> proc =
				new Procedure2<BandBuffer, UnsignedInt8Member>()
		{
			private byte[] buffer = new byte[1];
			
			@Override
			public void call(BandBuffer bandBuf, UnsignedInt8Member outVal) {
	
				bandBuf.getElemBytes(buffer);
				
				outVal.setV(buffer[0]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<SignedInt8Member>
	
		readBandedByteData(Dataset ds, SignedInt8Member var)
	{
		Procedure2<BandBuffer,SignedInt8Member> proc =
				new Procedure2<BandBuffer, SignedInt8Member>()
		{
			private byte[] buffer = new byte[1];
			
			@Override
			public void call(BandBuffer bandBuf, SignedInt8Member outVal) {
		
				bandBuf.getElemBytes(buffer);
				
				outVal.setV(buffer[0]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<UnsignedInt16Member>
	
		readBandedUShortData(Dataset ds, UnsignedInt16Member var)
	{
		Procedure2<BandBuffer,UnsignedInt16Member> proc =
				new Procedure2<BandBuffer, UnsignedInt16Member>()
		{
			private short[] buffer = new short[1];
			
			@Override
			public void call(BandBuffer bandBuf, UnsignedInt16Member outVal) {
		
				bandBuf.getElemShorts(buffer);
				
				outVal.setV(buffer[0]);
			}
		};

		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<SignedInt16Member>
	
		readBandedShortData(Dataset ds, SignedInt16Member var)
	{
		Procedure2<BandBuffer,SignedInt16Member> proc =
				new Procedure2<BandBuffer, SignedInt16Member>()
		{
			private short[] buffer = new short[1];
			
			@Override
			public void call(BandBuffer bandBuf, SignedInt16Member outVal) {
		
				bandBuf.getElemShorts(buffer);
				
				outVal.setV(buffer[0]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<UnsignedInt32Member>
	
		readBandedUIntData(Dataset ds, UnsignedInt32Member var)
	{
		Procedure2<BandBuffer,UnsignedInt32Member> proc =
				new Procedure2<BandBuffer, UnsignedInt32Member>()
		{
			private int[] buffer = new int[1];
			
			@Override
			public void call(BandBuffer bandBuf, UnsignedInt32Member outVal) {
		
				bandBuf.getElemInts(buffer);
				
				outVal.setV(buffer[0]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<SignedInt32Member>
	
		readBandedIntData(Dataset ds, SignedInt32Member var)
	{
		Procedure2<BandBuffer,SignedInt32Member> proc =
				new Procedure2<BandBuffer, SignedInt32Member>()
		{
			private int[] buffer = new int[1];
			
			@Override
			public void call(BandBuffer bandBuf, SignedInt32Member outVal) {
		
				bandBuf.getElemInts(buffer);
				
				outVal.setV(buffer[0]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<UnsignedInt64Member>
	
		readBandedUIntData(Dataset ds, UnsignedInt64Member var)
	{
		Procedure2<BandBuffer,UnsignedInt64Member> proc =
				new Procedure2<BandBuffer, UnsignedInt64Member>()
		{
			private long[] buffer = new long[1];
			
			@Override
			public void call(BandBuffer bandBuf, UnsignedInt64Member outVal) {
		
				bandBuf.getElemLongs(buffer);
				
				outVal.setV(buffer[0]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<SignedInt64Member>
	
		readBandedIntData(Dataset ds, SignedInt64Member var)
	{
		Procedure2<BandBuffer,SignedInt64Member> proc =
				new Procedure2<BandBuffer, SignedInt64Member>()
		{
			private long[] buffer = new long[1];
			
			@Override
			public void call(BandBuffer bandBuf, SignedInt64Member outVal) {
		
				bandBuf.getElemLongs(buffer);
				
				outVal.setV(buffer[0]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<Float32Member>
	
		readBandedFloatData(Dataset ds, Float32Member var)
	{
		Procedure2<BandBuffer,Float32Member> proc =
				new Procedure2<BandBuffer, Float32Member>()
		{
			private float[] buffer = new float[1];
			
			@Override
			public void call(BandBuffer bandBuf, Float32Member outVal) {
		
				bandBuf.getElemFloats(buffer);
				
				outVal.setV(buffer[0]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<Float64Member>
	
		readBandedDoubleData(Dataset ds, Float64Member var)
	{
		Procedure2<BandBuffer,Float64Member> proc =
				new Procedure2<BandBuffer, Float64Member>()
		{
			private double[] buffer = new double[1];

			@Override
			public void call(BandBuffer bandBuf, Float64Member outVal) {
		
				bandBuf.getElemDoubles(buffer);
				
				outVal.setV(buffer[0]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<GaussianInt16Member>
	
		readBandedGaussianShortData(Dataset ds, GaussianInt16Member var)
	{
		Procedure2<BandBuffer,GaussianInt16Member> proc =
				new Procedure2<BandBuffer, GaussianInt16Member>()
		{
			private short[] buffer = new short[2];

			@Override
			public void call(BandBuffer bandBuf, GaussianInt16Member outVal) {
		
				bandBuf.getElemShorts(buffer);
				
				outVal.setR((int) buffer[0]);
				
				outVal.setI((int) buffer[1]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<GaussianInt32Member>
	
		readBandedGaussianIntData(Dataset ds, GaussianInt32Member var)
	{
		Procedure2<BandBuffer,GaussianInt32Member> proc =
				new Procedure2<BandBuffer, GaussianInt32Member>()
		{
			private int[] buffer = new int[2];

			@Override
			public void call(BandBuffer bandBuf, GaussianInt32Member outVal) {
		
				bandBuf.getElemInts(buffer);
				
				outVal.setR(buffer[0]);
				
				outVal.setI(buffer[1]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<ComplexFloat32Member>
	
		readBandedComplexFloatData(Dataset ds, ComplexFloat32Member var)
	{
		Procedure2<BandBuffer,ComplexFloat32Member> proc =
				new Procedure2<BandBuffer, ComplexFloat32Member>()
		{
			private float[] buffer = new float[2];

			@Override
			public void call(BandBuffer bandBuf, ComplexFloat32Member outVal) {
		
				bandBuf.getElemFloats(buffer);
				
				outVal.setR(buffer[0]);
				
				outVal.setI(buffer[1]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}

	private static DimensionedDataSource<ComplexFloat64Member>
	
		readBandedComplexDoubleData(Dataset ds, ComplexFloat64Member var)
	{
		Procedure2<BandBuffer,ComplexFloat64Member> proc =
				new Procedure2<BandBuffer, ComplexFloat64Member>()
		{
			private double[] buffer = new double[2];

			@Override
			public void call(BandBuffer bandBuf, ComplexFloat64Member outVal) {
		
				bandBuf.getElemDoubles(buffer);
				
				outVal.setR(buffer[0]);
				
				outVal.setI(buffer[1]);
			}
		};
		
		return readBandedData(ds, var, proc);
	}
	
	
	private static class BandBuffer {
		
		private Object arr;
		
		private int readPtr;
		
		BandBuffer(Band band, int row, int elemsPerRow) {
			
			this.readPtr = 0;
			
			int type = band.getDataType();
			
			if (type == gdalconst.GDT_Byte) {
		
				arr = new byte[elemsPerRow * 1];
				
				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (byte[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_UInt16) {
				
				arr = new short[elemsPerRow * 1];
				
				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (short[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_Int16) {
				
				arr = new short[elemsPerRow * 1];
				
				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (short[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_UInt32) {
				
				arr = new int[elemsPerRow * 1];
				
				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (int[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_Int32) {
				
				arr = new int[elemsPerRow * 1];
				
				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (int[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_UInt64) {
				
				arr = new long[elemsPerRow * 1];

				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (long[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_Int64) {
				
				arr = new long[elemsPerRow * 1];

				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (long[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_Float32) {
				
				arr = new float[elemsPerRow * 1];
				
				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (float[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_Float64) {
				
				arr = new double[elemsPerRow * 1];
				
				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (double[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_CInt16) {
				
				arr = new short[elemsPerRow * 2];
				
				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (short[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_CInt32) {
				
				arr = new int[elemsPerRow * 2];
				
				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (int[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_CFloat32) {
				
				arr = new float[elemsPerRow * 2];
				
				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (float[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_CFloat64) {
				
				arr = new double[elemsPerRow * 2];
				
				band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (double[])arr, 0, 0);
			}
			else {
				
				throw new IllegalArgumentException("unknown data type in buffering");
			}
		}

		void getElemBytes(byte[] elem) {
			
			for (int i = 0; i < elem.length; i++) {
			
				elem[i] = Array.getByte(arr, readPtr++);
			}
		}
		
		void getElemShorts(short[] elem) {
			
			for (int i = 0; i < elem.length; i++) {
			
				elem[i] = Array.getShort(arr, readPtr++);
			}
		}
		
		void getElemInts(int[] elem) {
			
			for (int i = 0; i < elem.length; i++) {
			
				elem[i] = Array.getInt(arr, readPtr++);
			}
		}
		
		void getElemLongs(long[] elem) {
			
			for (int i = 0; i < elem.length; i++) {
			
				elem[i] = Array.getLong(arr, readPtr++);
			}
		}
		
		void getElemFloats(float[] elem) {
			
			for (int i = 0; i < elem.length; i++) {
			
				elem[i] = Array.getFloat(arr, readPtr++);
			}
		}
		
		void getElemDoubles(double[] elem) {
			
			for (int i = 0; i < elem.length; i++) {
			
				elem[i] = Array.getDouble(arr, readPtr++);
			}
		}
	}
}
