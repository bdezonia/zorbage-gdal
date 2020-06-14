/*
 * zorbage-gdal: code for loading gdal files into zorbage data structures for further processing
 *
 * Copyright (C) 2020 Barry DeZonia
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package nom.bdezonia.zorbage.gdal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;

import nom.bdezonia.zorbage.algebra.Allocatable;
import nom.bdezonia.zorbage.algebra.G;
import nom.bdezonia.zorbage.multidim.MultiDimDataSource;
import nom.bdezonia.zorbage.multidim.MultiDimStorage;
import nom.bdezonia.zorbage.procedure.Procedure4;
import nom.bdezonia.zorbage.sampling.IntegerIndex;
import nom.bdezonia.zorbage.sampling.SamplingCartesianIntegerGrid;
import nom.bdezonia.zorbage.sampling.SamplingIterator;
import nom.bdezonia.zorbage.type.float32.complex.ComplexFloat32Member;
import nom.bdezonia.zorbage.type.float32.real.Float32Member;
import nom.bdezonia.zorbage.type.float64.complex.ComplexFloat64Member;
import nom.bdezonia.zorbage.type.float64.real.Float64Member;
import nom.bdezonia.zorbage.type.int16.SignedInt16Member;
import nom.bdezonia.zorbage.type.int16.UnsignedInt16Member;
import nom.bdezonia.zorbage.type.int32.SignedInt32Member;
import nom.bdezonia.zorbage.type.int32.UnsignedInt32Member;
import nom.bdezonia.zorbage.type.int8.UnsignedInt8Member;

/**
 * 
 * @author Barry DeZonia
 *
 */
public class Gdal {

	/**
	 * 
	 */
	public static class DataBundle {
		public Map<String,String> chars;
		public List<MultiDimDataSource<UnsignedInt8Member>> uint8s;
		public List<MultiDimDataSource<SignedInt16Member>> int16s;
		public List<MultiDimDataSource<UnsignedInt16Member>> uint16s;
		public List<MultiDimDataSource<SignedInt32Member>> int32s;
		public List<MultiDimDataSource<UnsignedInt32Member>> uint32s;
		public List<MultiDimDataSource<Float32Member>> floats;
		public List<MultiDimDataSource<Float64Member>> doubles;
		public List<MultiDimDataSource<ComplexFloat32Member>> cfloats;  // including c16int
		public List<MultiDimDataSource<ComplexFloat64Member>> cdoubles; // including c32int
	}

	/**
	 * 
	 * @param newInfo
	 * @param results
	 */
	static void mergeBundleInto(DataBundle newInfo, DataBundle results) {

		if (results.chars == null) {
			results.chars = newInfo.chars;
		}
		else {
			if (newInfo.chars != null) {
				for (String key : newInfo.chars.keySet()) {
					results.chars.put(key, newInfo.chars.get(key));
				}
			}
		}

		if (results.uint8s == null) {
			results.uint8s = newInfo.uint8s;
		}
		else {
			if (newInfo.uint8s != null) {
				results.uint8s.addAll(newInfo.uint8s);
			}
		}

		if (results.uint16s == null) {
			results.uint16s = newInfo.uint16s;
		}
		else {
			if (newInfo.uint16s != null) {
				results.uint16s.addAll(newInfo.uint16s);
			}
		}

		if (results.int16s == null) {
			results.int16s = newInfo.int16s;
		}
		else {
			if (newInfo.int16s != null) {
				results.int16s.addAll(newInfo.int16s);
			}
		}

		if (results.uint32s == null) {
			results.uint32s = newInfo.uint32s;
		}
		else {
			if (newInfo.uint32s != null) {
				results.uint32s.addAll(newInfo.uint32s);
			}
		}

		if (results.int32s == null) {
			results.int32s = newInfo.int32s;
		}
		else {
			if (newInfo.int32s != null) {
				results.int32s.addAll(newInfo.int32s);
			}
		}

		if (results.floats == null) {
			results.floats = newInfo.floats;
		}
		else {
			if (newInfo.floats != null) {
				results.floats.addAll(newInfo.floats);
			}
		}

		if (results.doubles == null) {
			results.doubles = newInfo.doubles;
		}
		else {
			if (newInfo.doubles != null) {
				results.doubles.addAll(newInfo.doubles);
			}
		}

		if (results.cfloats == null) {
			results.cfloats = newInfo.cfloats;
		}
		else {
			if (newInfo.cfloats != null) {
				results.cfloats.addAll(newInfo.cfloats);
			}
		}

		if (results.cdoubles == null) {
			results.cdoubles = newInfo.cdoubles;
		}
		else {
			if (newInfo.cdoubles != null) {
				results.cdoubles.addAll(newInfo.cdoubles);
			}
		}

	}
	
	/**
	 * This must be called once at startup by users of this gdal interface package
	 */
	public static void init() {
		gdal.AllRegister();
	}
	
	/**
	 * 
	 * @param filename
	 */
	public static DataBundle loadAll(String filename) {

		DataBundle resultSets = new DataBundle();
		Dataset ds = gdal.Open(filename);
		@SuppressWarnings("unchecked")
		Vector<String> subdatasetInfo = (Vector<String>) ds.GetMetadata_List("SUBDATASETS");
		int counter = 1;
		for (String entry : subdatasetInfo) {
			String namePrefix = "SUBDATASET_" + counter + "_NAME=";
			if (entry.startsWith(namePrefix)) {
				String[] pair = entry.split("=");
				if (pair.length != 2)
					throw new IllegalArgumentException("gdal metadata: too many equal signs in internal filename");
				DataBundle bundle = loadAll(pair[1]);
				mergeBundleInto(bundle, resultSets);
				counter++;
			}
		}
		System.out.println(ds.GetDescription());
		if (ds.GetRasterCount() == 0) {
			// this file probably contains a bunch of subdatasets. somehow query what they are
			// and use gdal.Open() on each of them.
			System.out.println("this dataset has been encoded as a bunch of subdatasets");
		}
		else {
			DataBundle bundle = new DataBundle();
			
			// report the dataset's info
			System.out.println(" x size " + ds.GetRasterXSize());
			System.out.println(" y size " + ds.GetRasterYSize());
			System.out.println(" rasters " + ds.GetRasterCount());
			int type = -1;
			int xSize = ds.GetRasterXSize();
			int ySize = ds.getRasterYSize();
			for (int i = 1; i <= ds.GetRasterCount(); i++) {
				Band band = ds.GetRasterBand(i);
				if (type == -1)
					type = band.GetRasterDataType();
				if (band.GetRasterDataType() != type)
					throw new IllegalArgumentException("data has multiple different band types!");
				if ((band.GetXSize() != xSize) || (band.GetYSize() != ySize))
					throw new IllegalArgumentException("data has multiple band resolutions!");
				System.out.println("   band " + i + " " + gdal.GetDataTypeName(type) + " " + band.GetDescription());
			}
			if (type == gdalconst.GDT_Byte) {
				if (bundle.uint8s == null) bundle.uint8s = new ArrayList<>();
				bundle.uint8s.add(loadUByteData(ds, G.UINT8.construct()));
			}
			else if (type == gdalconst.GDT_UInt16) {
				if (bundle.uint16s == null) bundle.uint16s = new ArrayList<>();
				bundle.uint16s.add(loadUShortData(ds, G.UINT16.construct()));
			}
			else if (type == gdalconst.GDT_Int16) {
				if (bundle.int16s == null) bundle.int16s = new ArrayList<>();
				bundle.int16s.add(loadShortData(ds, G.INT16.construct()));
			}
			else if (type == gdalconst.GDT_UInt32) {
				if (bundle.uint32s == null) bundle.uint32s = new ArrayList<>();
				bundle.uint32s.add(loadUIntData(ds, G.UINT32.construct()));
			}
			else if (type == gdalconst.GDT_Int32) {
				if (bundle.int32s == null) bundle.int32s = new ArrayList<>();
				bundle.int32s.add(loadIntData(ds, G.INT32.construct()));
			}
			else if (type == gdalconst.GDT_Float32) {
				if (bundle.floats == null) bundle.floats = new ArrayList<>();
				bundle.floats.add(loadFloatData(ds, G.FLT.construct()));
			}
			else if (type == gdalconst.GDT_Float64) {
				if (bundle.doubles == null) bundle.doubles = new ArrayList<>();
				bundle.doubles.add(loadDoubleData(ds, G.DBL.construct()));
			}
			else if (type == gdalconst.GDT_CInt16) {
				// I have no exact match for this class: widen data
				if (bundle.cfloats == null) bundle.cfloats = new ArrayList<>();
				bundle.cfloats.add(loadComplexFloatData(ds, G.CFLT.construct()));
			}
			else if (type == gdalconst.GDT_CInt32) {
				// I have no exact match for this class: widen data
				if (bundle.cdoubles == null) bundle.cdoubles = new ArrayList<>();
				bundle.cdoubles.add(loadComplexDoubleData(ds, G.CDBL.construct()));
			}
			else if (type == gdalconst.GDT_CFloat32) {
				if (bundle.cfloats == null) bundle.cfloats = new ArrayList<>();
				bundle.cfloats.add(loadComplexFloatData(ds, G.CFLT.construct()));
			}
			else if (type == gdalconst.GDT_CFloat64) {
				if (bundle.cdoubles == null) bundle.cdoubles = new ArrayList<>();
				bundle.cdoubles.add(loadComplexDoubleData(ds, G.CDBL.construct()));
			}
			else
				System.out.println("Ignoring unknown data type "+gdal.GetDataTypeName(type));
			System.out.println("data loaded");
			mergeBundleInto(bundle, resultSets);
		}
		return resultSets;
	}
	
	private static <U extends Allocatable<U>>
		MultiDimDataSource<U> loadData(Dataset ds, U var, Procedure4<Band, Integer, Integer, U> proc)
	{
		int planes = ds.getRasterCount();
		long[] dims;
		if (planes == 1) {
			dims = new long[] {ds.getRasterXSize(), ds.GetRasterYSize()};
		}
		else {
			dims = new long[] {ds.getRasterXSize(), ds.GetRasterYSize(), planes};
		}
		MultiDimDataSource<U> data = MultiDimStorage.allocate(dims, var);
		
		long[] minPt = new long[data.numDimensions()];
		long[] maxPt = new long[data.numDimensions()];
		for (int i = 0; i < data.numDimensions(); i++) {
			maxPt[i] = data.dimension(i) - 1;
		}
		for (int i = 0; i < planes; i++) {
			if (planes > 1) {
				minPt[2] = i;
				maxPt[2] = i;
			}
			Band band = ds.GetRasterBand(i+1);
			SamplingCartesianIntegerGrid grid = new SamplingCartesianIntegerGrid(minPt, maxPt);
			if (planes > 1) {
				minPt[2] = 0;
				maxPt[2] = data.dimension(i)-1;
			}
			SamplingIterator<IntegerIndex> iter = grid.iterator();
			IntegerIndex index = new IntegerIndex(data.numDimensions());
			// gdal y origin is top left of raster while zorbage has it at lower left
			for (int y = ds.GetRasterYSize()-1; y >= 0; y--) {
				for (int x = 0; x < ds.GetRasterXSize(); x++) {
					iter.next(index);
					proc.call(band, x, y, var);
					data.set(index, var);
				}				
			}
		}
		return data;
	}

	private static MultiDimDataSource<UnsignedInt8Member> loadUByteData(Dataset ds, UnsignedInt8Member var) {
		Procedure4<Band,Integer,Integer,UnsignedInt8Member> proc =
				new Procedure4<Band, Integer, Integer, UnsignedInt8Member>()
		{
			private byte[] buffer = new byte[1];
			
			@Override
			public void call(Band band, Integer x, Integer y, UnsignedInt8Member outVal) {
				band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), buffer, 0, 0);
				outVal.setV(buffer[0]);
			}
		};
		return loadData(ds, var, proc);
	}

	private static MultiDimDataSource<UnsignedInt16Member> loadUShortData(Dataset ds, UnsignedInt16Member var) {
		Procedure4<Band,Integer,Integer,UnsignedInt16Member> proc =
				new Procedure4<Band, Integer, Integer, UnsignedInt16Member>()
		{
			private short[] buffer = new short[1];
			
			@Override
			public void call(Band band, Integer x, Integer y, UnsignedInt16Member outVal) {
				band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), buffer, 0, 0);
				outVal.setV(buffer[0]);
			}
		};
		return loadData(ds, var, proc);
	}

	private static MultiDimDataSource<SignedInt16Member> loadShortData(Dataset ds, SignedInt16Member var) {
		Procedure4<Band,Integer,Integer,SignedInt16Member> proc =
				new Procedure4<Band, Integer, Integer, SignedInt16Member>()
		{
			private short[] buffer = new short[1];
			
			@Override
			public void call(Band band, Integer x, Integer y, SignedInt16Member outVal) {
				band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), buffer, 0, 0);
				outVal.setV(buffer[0]);
			}
		};
		return loadData(ds, var, proc);
	}

	private static MultiDimDataSource<UnsignedInt32Member> loadUIntData(Dataset ds, UnsignedInt32Member var) {
		Procedure4<Band,Integer,Integer,UnsignedInt32Member> proc =
				new Procedure4<Band, Integer, Integer, UnsignedInt32Member>()
		{
			private int[] buffer = new int[1];
			
			@Override
			public void call(Band band, Integer x, Integer y, UnsignedInt32Member outVal) {
				band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), buffer, 0, 0);
				outVal.setV(buffer[0]);
			}
		};
		return loadData(ds, var, proc);
	}

	private static MultiDimDataSource<SignedInt32Member> loadIntData(Dataset ds, SignedInt32Member var) {
		Procedure4<Band,Integer,Integer,SignedInt32Member> proc =
				new Procedure4<Band, Integer, Integer, SignedInt32Member>()
		{
			private int[] buffer = new int[1];
			
			@Override
			public void call(Band band, Integer x, Integer y, SignedInt32Member outVal) {
				band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), buffer, 0, 0);
				outVal.setV(buffer[0]);
			}
		};
		return loadData(ds, var, proc);
	}

	private static MultiDimDataSource<Float32Member> loadFloatData(Dataset ds, Float32Member var) {
		Procedure4<Band,Integer,Integer,Float32Member> proc =
				new Procedure4<Band, Integer, Integer, Float32Member>()
		{
			private float[] buffer = new float[1];
			
			@Override
			public void call(Band band, Integer x, Integer y, Float32Member outVal) {
				band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), buffer, 0, 0);
				outVal.setV(buffer[0]);
			}
		};
		return loadData(ds, var, proc);
	}

	private static MultiDimDataSource<Float64Member> loadDoubleData(Dataset ds, Float64Member var) {
		Procedure4<Band,Integer,Integer,Float64Member> proc =
				new Procedure4<Band, Integer, Integer, Float64Member>()
		{
			private double[] buffer = new double[1];

			@Override
			public void call(Band band, Integer x, Integer y, Float64Member outVal) {
				band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), buffer, 0, 0);
				outVal.setV(buffer[0]);
			}
		};
		return loadData(ds, var, proc);
	}

	private static MultiDimDataSource<ComplexFloat32Member> loadComplexFloatData(Dataset ds, ComplexFloat32Member var) {
		Procedure4<Band,Integer,Integer,ComplexFloat32Member> proc =
				new Procedure4<Band, Integer, Integer, ComplexFloat32Member>()
		{
			private short[] sbuffer = new short[2];
			private float[] fbuffer = new float[2];

			@Override
			public void call(Band band, Integer x, Integer y, ComplexFloat32Member outVal) {
				if (band.getDataType() == gdalconst.GDT_CInt16) {
					band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), sbuffer, 0, 0);
					outVal.setR(sbuffer[0]);
					outVal.setI(sbuffer[1]);
				}
				else {  // data type = GDT_CFloat32
					band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), fbuffer, 0, 0);
					outVal.setR(fbuffer[0]);
					outVal.setI(fbuffer[1]);
				}
			}
		};
		return loadData(ds, var, proc);
	}

	private static MultiDimDataSource<ComplexFloat64Member> loadComplexDoubleData(Dataset ds, ComplexFloat64Member var) {
		Procedure4<Band,Integer,Integer,ComplexFloat64Member> proc =
				new Procedure4<Band, Integer, Integer, ComplexFloat64Member>()
		{
			private int[] ibuffer = new int[2];
			private double[] dbuffer = new double[2];

			@Override
			public void call(Band band, Integer x, Integer y, ComplexFloat64Member outVal) {
				if (band.getDataType() == gdalconst.GDT_CInt32) {
					band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), ibuffer, 0, 0);
					outVal.setR(ibuffer[0]);
					outVal.setI(ibuffer[1]);
				}
				else {  // data type = GDT_CFloat64
					band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), dbuffer, 0, 0);
					outVal.setR(dbuffer[0]);
					outVal.setI(dbuffer[1]);
				}
			}
		};
		return loadData(ds, var, proc);
	}
	
}
