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
import nom.bdezonia.zorbage.algebra.SetFromDoubles;
import nom.bdezonia.zorbage.algebra.SetFromFloats;
import nom.bdezonia.zorbage.algebra.SetFromInts;
import nom.bdezonia.zorbage.algebra.SetFromLongs;
import nom.bdezonia.zorbage.algebra.SetFromShorts;
import nom.bdezonia.zorbage.coordinates.CoordinateSpace;
import nom.bdezonia.zorbage.coordinates.LinearNdCoordinateSpace;
import nom.bdezonia.zorbage.data.DimensionedDataSource;
import nom.bdezonia.zorbage.data.DimensionedStorage;
import nom.bdezonia.zorbage.dataview.PlaneView;
import nom.bdezonia.zorbage.misc.DataBundle;
import nom.bdezonia.zorbage.procedure.Procedure2;
import nom.bdezonia.zorbage.sampling.IntegerIndex;
import nom.bdezonia.zorbage.sampling.SamplingCartesianIntegerGrid;
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
					System.out.println("  axis "+k+" scale "+scales[k]+" offset "+offsets[k]);
				}

				System.out.println("  name "+data.GetName());
				
				System.out.println("  full name "+data.GetFullName());
				
				System.out.println("  unit "+data.GetUnit());
				
				System.out.println("  data type "+gdal.GetDataTypeName(data.GetDataType().GetNumericDataType()));
				
				System.out.println("  and has " + data.GetDimensionCount() + " dimensions.");
				
				System.out.println("  and structural info:");
				
				Hashtable<String,?> ht = data.GetStructuralInfo();
				
				Enumeration<String> keys = ht.keys();
				
				Iterator<String> iter = keys.asIterator();
				
				while (iter.hasNext()) {
					
					String key = iter.next();
					
					System.out.println("    key "+key+" value "+ht.get(key));
				}
				
				int type = data.GetDataType().GetNumericDataType();
				
				if (type == gdalconst.GDT_Byte) {

					outputs.mergeUInt8(loadUByteData(data, G.UINT8.construct()));
				}
				else if (type == gdalconst.GDT_Int8) {

					outputs.mergeInt8(loadByteData(data, G.INT8.construct()));
				}
				else if (type == gdalconst.GDT_UInt16) {
					
					outputs.mergeUInt16(loadUShortData(data, G.UINT16.construct()));
				}
				else if (type == gdalconst.GDT_Int16) {
					
					outputs.mergeInt16(loadShortData(data, G.INT16.construct()));
				}
				else if (type == gdalconst.GDT_UInt32) {
					
					outputs.mergeUInt32(loadUIntData(data, G.UINT32.construct()));
				}
				else if (type == gdalconst.GDT_Int32) {
					
					outputs.mergeInt32(loadIntData(data, G.INT32.construct()));
				}
				else if (type == gdalconst.GDT_UInt64) {
					
					outputs.mergeUInt64(loadULongData(data, G.UINT64.construct()));
				}
				else if (type == gdalconst.GDT_Int64) {
					
					outputs.mergeInt64(loadLongData(data, G.INT64.construct()));
				}
				else if (type == gdalconst.GDT_Float32) {
					
					outputs.mergeFlt32(loadFloatData(data, G.FLT.construct()));
				}
				else if (type == gdalconst.GDT_Float64) {
					
					outputs.mergeFlt64(loadDoubleData(data, G.DBL.construct()));
				}
				else if (type == gdalconst.GDT_CInt16) {
					
					outputs.mergeGaussianInt16(loadGaussianShortData(data, G.GAUSS16.construct()));
				}
				else if (type == gdalconst.GDT_CInt32) {
					
					outputs.mergeGaussianInt32(loadGaussianIntData(data, G.GAUSS32.construct()));
				}
				else if (type == gdalconst.GDT_CFloat32) {
					
					outputs.mergeComplexFlt32(loadComplexFloatData(data, G.CFLT.construct()));
				}
				else if (type == gdalconst.GDT_CFloat64) {
					
					outputs.mergeComplexFlt64(loadComplexDoubleData(data, G.CDBL.construct()));
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
	
				outputs.mergeUInt8(loadUByteData(ds, G.UINT8.construct()));
			}
			else if (type == gdalconst.GDT_Int8) {
	
				outputs.mergeInt8(loadByteData(ds, G.INT8.construct()));
			}
			else if (type == gdalconst.GDT_UInt16) {
				
				outputs.mergeUInt16(loadUShortData(ds, G.UINT16.construct()));
			}
			else if (type == gdalconst.GDT_Int16) {
				
				outputs.mergeInt16(loadShortData(ds, G.INT16.construct()));
			}
			else if (type == gdalconst.GDT_UInt32) {
				
				outputs.mergeUInt32(loadUIntData(ds, G.UINT32.construct()));
			}
			else if (type == gdalconst.GDT_Int32) {
				
				outputs.mergeInt32(loadIntData(ds, G.INT32.construct()));
			}
			else if (type == gdalconst.GDT_UInt64) {
				
				outputs.mergeUInt64(loadUIntData(ds, G.UINT64.construct()));
			}
			else if (type == gdalconst.GDT_Int64) {
				
				outputs.mergeInt64(loadIntData(ds, G.INT64.construct()));
			}
			else if (type == gdalconst.GDT_Float32) {
				
				outputs.mergeFlt32(loadFloatData(ds, G.FLT.construct()));
			}
			else if (type == gdalconst.GDT_Float64) {
				
				outputs.mergeFlt64(loadDoubleData(ds, G.DBL.construct()));
			}
			else if (type == gdalconst.GDT_CInt16) {
				
				outputs.mergeGaussianInt16(loadGaussianShortData(ds, G.GAUSS16.construct()));
			}
			else if (type == gdalconst.GDT_CInt32) {
				
				outputs.mergeGaussianInt32(loadGaussianIntData(ds, G.GAUSS32.construct()));
			}
			else if (type == gdalconst.GDT_CFloat32) {
				
				outputs.mergeComplexFlt32(loadComplexFloatData(ds, G.CFLT.construct()));
			}
			else if (type == gdalconst.GDT_CFloat64) {
				
				outputs.mergeComplexFlt64(loadComplexDoubleData(ds, G.CDBL.construct()));
			}
			else if (type != -1) {
			
				System.out.println("Ignoring unknown data type "+gdal.GetDataTypeName(type));
			}
		}

		return outputs;
	}
	
	private static <U extends Allocatable<U>> DimensionedDataSource<U>
	
		loadData(Dataset ds, U var, Procedure2<BandBuffer, U> proc)
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

	interface BufferIO<U> {
		
		void readGdalDataIntoBuffer(MDArray data, long[] gdalCoords, long[] ones);
		void readFromBufferIntoValue(U value);
	}
	
	private static class ByteBufferIO<U extends SetFromBytes> implements BufferIO<U> {
		
		private byte[] buffer;
		
		ByteBufferIO(int bufSize) {
			
			buffer = new byte[bufSize];
		}
		
		@Override
		public void readGdalDataIntoBuffer(MDArray data, long[] gdalCoords, long[] ones) {
			
			data.Read(gdalCoords, ones, buffer);
		}
		
		@Override
		public void readFromBufferIntoValue(U value) {
			
			value.setFromBytes(buffer);
		}
		
	}
	
	private static class ShortBufferIO<U extends SetFromShorts> implements BufferIO<U> {
		
		private short[] buffer;
		
		ShortBufferIO(int bufSize) {
			
			buffer = new short[bufSize];
		}
		
		@Override
		public void readGdalDataIntoBuffer(MDArray data, long[] gdalCoords, long[] ones) {
			
			data.Read(gdalCoords, ones, buffer);
		}
		
		@Override
		public void readFromBufferIntoValue(U value) {
			
			value.setFromShorts(buffer);
		}
		
	}
	
	private static class IntBufferIO<U extends SetFromInts> implements BufferIO<U> {
		
		private int[] buffer;
		
		IntBufferIO(int bufSize) {
			
			buffer = new int[bufSize];
		}
		
		@Override
		public void readGdalDataIntoBuffer(MDArray data, long[] gdalCoords, long[] ones) {
			
			data.Read(gdalCoords, ones, buffer);
		}
		
		@Override
		public void readFromBufferIntoValue(U value) {
			
			value.setFromInts(buffer);
		}
		
	}
	
	private static class LongBufferIO<U extends SetFromLongs> implements BufferIO<U> {
		
		private long[] buffer;
		
		LongBufferIO(int bufSize) {
			
			buffer = new long[bufSize];
		}
		
		@Override
		public void readGdalDataIntoBuffer(MDArray data, long[] gdalCoords, long[] ones) {
			
			data.Read(gdalCoords, ones, buffer);
		}
		
		@Override
		public void readFromBufferIntoValue(U value) {
			
			value.setFromLongs(buffer);
		}
		
	}
	
	private static class FloatBufferIO<U extends SetFromFloats> implements BufferIO<U> {
		
		private float[] buffer;
		
		FloatBufferIO(int bufSize) {
			
			buffer = new float[bufSize];
		}
		
		@Override
		public void readGdalDataIntoBuffer(MDArray data, long[] gdalCoords, long[] ones) {
			
			data.Read(gdalCoords, ones, buffer);
		}
		
		@Override
		public void readFromBufferIntoValue(U value) {
			
			value.setFromFloats(buffer);
		}
		
	}
	
	private static class DoubleBufferIO<U extends SetFromDoubles> implements BufferIO<U> {
		
		private double[] buffer;
		
		DoubleBufferIO(int bufSize) {
			
			buffer = new double[bufSize];
		}
		
		@Override
		public void readGdalDataIntoBuffer(MDArray data, long[] gdalCoords, long[] ones) {
			
			data.Read(gdalCoords, ones, buffer);
		}
		
		@Override
		public void readFromBufferIntoValue(U value) {
			
			value.setFromDoubles(buffer);
		}
		
	}
	
	private static <T extends Algebra<T,U>, U extends Allocatable<U>>
	
		DimensionedDataSource<U> readData(MDArray data, U type, BufferIO<U> io)
	{
		Dimension[] dims = data.GetDimensions();

		long[] gdalDims = new long[dims.length];
		
		long[] zorbDims = new long[dims.length];

		for (int i = 0; i < dims.length; i++) {
			
			gdalDims[i] = dims[i].GetSize();
			
			zorbDims[dims.length - 1 - i] = gdalDims[i];
		}
		
		DimensionedDataSource<U> output =
				
				DimensionedStorage.allocate(type.allocate(), zorbDims);
		
		U val = type.allocate();
		
		IntegerIndex idx = new IntegerIndex(dims.length);
		
		long[] gdalCoords = new long[dims.length];
		
		IntegerIndex zorbCoords = new IntegerIndex(dims.length);
		
		long[] ones = new long[dims.length];
		
		for (int i = 0; i < ones.length; i++) {
			ones[i] = 1;
		}

		nom.bdezonia.zorbage.sampling.SamplingIterator<IntegerIndex> iter =
				
				new SamplingCartesianIntegerGrid(gdalDims).iterator();

		while (iter.hasNext()) {
			
			iter.next(idx);
			
			for (int i = 0; i < dims.length; i++) {
				
				gdalCoords[i] = idx.get(i);
				
				zorbCoords.set(dims.length - 1 - i, gdalCoords[i]);
			}
			
			io.readGdalDataIntoBuffer(data, gdalCoords, ones);
			
			io.readFromBufferIntoValue(val);
			
			output.set(zorbCoords, val);
		}
		
		output.setName(data.GetName());
		output.setSource(data.GetFullName());
		output.setValueUnit(data.GetUnit());
		output.setValueType("unknown type");

		Double[] scales = new Double[dims.length];
		Double[] offsets = new Double[dims.length];
		
		data.GetScale(scales);
		data.GetOffset(offsets);
		
		BigDecimal[] bdScales = new BigDecimal[dims.length];
		BigDecimal[] bdOffsets = new BigDecimal[dims.length];

		for (int i = 0; i < dims.length; i++) {
			
			bdScales[i] = BigDecimal.valueOf(scales[i]);
			bdOffsets[i] = BigDecimal.valueOf(offsets[i]);
		}
		
		LinearNdCoordinateSpace space = new LinearNdCoordinateSpace(bdScales, bdOffsets);

		output.setCoordinateSpace(space);
		
		// TODO set MetaData based upon gdal attributes?????
		
		return output;
	}
	
	private static DimensionedDataSource<UnsignedInt8Member>
	
		loadUByteData(MDArray data, UnsignedInt8Member var)
	{
		return readData(data, var, new ByteBufferIO<UnsignedInt8Member>(1));
	}

	private static DimensionedDataSource<SignedInt8Member>
	
		loadByteData(MDArray data, SignedInt8Member var)
	{
		return readData(data, var, new ByteBufferIO<SignedInt8Member>(1));
	}

	private static DimensionedDataSource<UnsignedInt16Member>
	
		loadUShortData(MDArray data, UnsignedInt16Member var)
	{
		return readData(data, var, new ShortBufferIO<UnsignedInt16Member>(1));
	}

	private static DimensionedDataSource<SignedInt16Member>
	
		loadShortData(MDArray data, SignedInt16Member var)
	{
		return readData(data, var, new ShortBufferIO<SignedInt16Member>(1));
	}

	private static DimensionedDataSource<UnsignedInt32Member>
	
		loadUIntData(MDArray data, UnsignedInt32Member var)
	{
		return readData(data, var, new IntBufferIO<UnsignedInt32Member>(1));
	}

	private static DimensionedDataSource<SignedInt32Member>
	
		loadIntData(MDArray data, SignedInt32Member var)
	{
		return readData(data, var, new IntBufferIO<SignedInt32Member>(1));
	}

	private static DimensionedDataSource<UnsignedInt64Member>
	
		loadULongData(MDArray data, UnsignedInt64Member var)
	{
		return readData(data, var, new LongBufferIO<UnsignedInt64Member>(1));
	}

	private static DimensionedDataSource<SignedInt64Member>
	
		loadLongData(MDArray data, SignedInt64Member var)
	{
		return readData(data, var, new LongBufferIO<SignedInt64Member>(1));
	}

	private static DimensionedDataSource<Float32Member>
	
		loadFloatData(MDArray data, Float32Member var)
	{
		return readData(data, var, new FloatBufferIO<Float32Member>(1));
	}

	private static DimensionedDataSource<Float64Member>
	
		loadDoubleData(MDArray data, Float64Member var)
	{
		return readData(data, var, new DoubleBufferIO<Float64Member>(1));
	}

	private static DimensionedDataSource<ComplexFloat32Member>
	
		loadComplexFloatData(MDArray data, ComplexFloat32Member var)
	{
		return readData(data, var, new FloatBufferIO<ComplexFloat32Member>(2));
	}

	private static DimensionedDataSource<ComplexFloat64Member>
	
		loadComplexDoubleData(MDArray data, ComplexFloat64Member var)
	{
		return readData(data, var, new DoubleBufferIO<ComplexFloat64Member>(2));
	}

	private static DimensionedDataSource<GaussianInt16Member>
	
		loadGaussianShortData(MDArray data, GaussianInt16Member var)
	{
		return readData(data, var, new ShortBufferIO<GaussianInt16Member>(2));
	}

	private static DimensionedDataSource<GaussianInt32Member>
	
		loadGaussianIntData(MDArray data, GaussianInt32Member var)
	{
		return readData(data, var, new IntBufferIO<GaussianInt32Member>(2));
	}

	private static DimensionedDataSource<UnsignedInt8Member>
	
		loadUByteData(Dataset ds, UnsignedInt8Member var)
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
		
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<SignedInt8Member>
	
		loadByteData(Dataset ds, SignedInt8Member var)
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
		
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<UnsignedInt16Member>
	
		loadUShortData(Dataset ds, UnsignedInt16Member var)
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

		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<SignedInt16Member>
	
		loadShortData(Dataset ds, SignedInt16Member var)
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
		
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<UnsignedInt32Member>
	
		loadUIntData(Dataset ds, UnsignedInt32Member var)
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
		
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<SignedInt32Member>
	
		loadIntData(Dataset ds, SignedInt32Member var)
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
		
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<UnsignedInt64Member>
	
		loadUIntData(Dataset ds, UnsignedInt64Member var)
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
		
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<SignedInt64Member>
	
		loadIntData(Dataset ds, SignedInt64Member var)
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
		
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<Float32Member>
	
		loadFloatData(Dataset ds, Float32Member var)
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
		
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<Float64Member>
	
		loadDoubleData(Dataset ds, Float64Member var)
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
		
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<GaussianInt16Member>
	
		loadGaussianShortData(Dataset ds, GaussianInt16Member var)
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
		
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<GaussianInt32Member>
	
		loadGaussianIntData(Dataset ds, GaussianInt32Member var)
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
		
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<ComplexFloat32Member>
	
		loadComplexFloatData(Dataset ds, ComplexFloat32Member var)
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
		
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<ComplexFloat64Member>
	
		loadComplexDoubleData(Dataset ds, ComplexFloat64Member var)
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
		
		return loadData(ds, var, proc);
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

				// TODO remove exception and uncomment the line after it
				
				throw new UnsupportedOperationException("This code won't work until gdal java api jar 3.8.0 has been released");

				//band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (long[])arr, 0, 0);
			}
			else if (type == gdalconst.GDT_Int64) {
				
				arr = new long[elemsPerRow * 1];

				// TODO remove exception and uncomment the line after it
				
				throw new UnsupportedOperationException("This code won't work until gdal java api jar 3.8.0 has been released");

				//band.ReadRaster(0, row, elemsPerRow, 1, elemsPerRow, 1, band.getDataType(), (long[])arr, 0, 0);
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
