/*
 * zorbage-gdal: code for using the gdal data file library to open files into zorbage data structures for further processing
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

import java.util.Hashtable;
import java.util.Vector;

import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;

import nom.bdezonia.zorbage.algebra.Allocatable;
import nom.bdezonia.zorbage.algebra.G;
import nom.bdezonia.zorbage.algorithm.GridIterator;
import nom.bdezonia.zorbage.data.DimensionedDataSource;
import nom.bdezonia.zorbage.data.DimensionedStorage;
import nom.bdezonia.zorbage.misc.DataBundle;
import nom.bdezonia.zorbage.procedure.Procedure4;
import nom.bdezonia.zorbage.sampling.IntegerIndex;
import nom.bdezonia.zorbage.sampling.SamplingIterator;
import nom.bdezonia.zorbage.type.complex.float32.ComplexFloat32Member;
import nom.bdezonia.zorbage.type.complex.float64.ComplexFloat64Member;
import nom.bdezonia.zorbage.type.gaussian.int16.GaussianInt16Member;
import nom.bdezonia.zorbage.type.gaussian.int32.GaussianInt32Member;
import nom.bdezonia.zorbage.type.integer.int16.SignedInt16Member;
import nom.bdezonia.zorbage.type.integer.int16.UnsignedInt16Member;
import nom.bdezonia.zorbage.type.integer.int32.SignedInt32Member;
import nom.bdezonia.zorbage.type.integer.int32.UnsignedInt32Member;
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
	public static void init() {
		gdal.AllRegister();
	}
	
	/**
	 * 
	 * @param filename
	 */
	public static DataBundle loadAllDatasets(String filename) {

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
				DataBundle bundle = loadAllDatasets(pair[1]);
				resultSets.mergeAll(bundle);
				counter++;
			}
		}

		DataBundle bundle = new DataBundle();
		
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
		}
		if (type == gdalconst.GDT_Byte) {
			bundle.mergeUInt8(loadUByteData(ds, G.UINT8.construct()));
		}
		else if (type == gdalconst.GDT_UInt16) {
			bundle.mergeUInt16(loadUShortData(ds, G.UINT16.construct()));
		}
		else if (type == gdalconst.GDT_Int16) {
			bundle.mergeInt16(loadShortData(ds, G.INT16.construct()));
		}
		else if (type == gdalconst.GDT_UInt32) {
			bundle.mergeUInt32(loadUIntData(ds, G.UINT32.construct()));
		}
		else if (type == gdalconst.GDT_Int32) {
			bundle.mergeInt32(loadIntData(ds, G.INT32.construct()));
		}
		else if (type == gdalconst.GDT_Float32) {
			bundle.mergeFlt32(loadFloatData(ds, G.FLT.construct()));
		}
		else if (type == gdalconst.GDT_Float64) {
			bundle.mergeFlt64(loadDoubleData(ds, G.DBL.construct()));
		}
		else if (type == gdalconst.GDT_CInt16) {
			bundle.mergeGaussianInt16(loadGaussianShortData(ds, G.GAUSS16.construct()));
		}
		else if (type == gdalconst.GDT_CInt32) {
			bundle.mergeGaussianInt32(loadGaussianIntData(ds, G.GAUSS32.construct()));
		}
		else if (type == gdalconst.GDT_CFloat32) {
			bundle.mergeComplexFlt32(loadComplexFloatData(ds, G.CFLT.construct()));
		}
		else if (type == gdalconst.GDT_CFloat64) {
			bundle.mergeComplexFlt64(loadComplexDoubleData(ds, G.CDBL.construct()));
		}
		else if (type != -1)
			System.out.println("Ignoring unknown data type "+gdal.GetDataTypeName(type));
		resultSets.mergeAll(bundle);
		return resultSets;
	}
	
	private static <U extends Allocatable<U>>
		DimensionedDataSource<U> loadData(Dataset ds, U var, Procedure4<Band, Integer, Integer, U> proc)
	{
		int planes = ds.getRasterCount();
		long[] dims;
		if (planes == 1) {
			dims = new long[] {ds.getRasterXSize(), ds.GetRasterYSize()};
		}
		else {
			dims = new long[] {ds.getRasterXSize(), ds.GetRasterYSize(), planes};
		}
		
		DimensionedDataSource<U> data = DimensionedStorage.allocate(var, dims);
		
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
			data.metadata().put("band-"+i+"-description", band.GetDescription());
			data.metadata().put("band-"+i+"-units", band.GetUnitType());
			@SuppressWarnings("unchecked")
			Hashtable<String,String> table = (Hashtable<String,String>) band.GetMetadata_Dict();
			for (String key : table.keySet()) {
				String value = table.get(key);
				if (key != null && key.length() > 0) {
					if (value != null && value.length() > 0) {
						data.metadata().put("band-"+i+"-"+key, value);
					}
				}
			}
			SamplingIterator<IntegerIndex> iter = GridIterator.compute(minPt, maxPt);
			IntegerIndex index = new IntegerIndex(data.numDimensions());
			for (int y = 0; y < ds.GetRasterYSize(); y++) {
				for (int x = 0; x < ds.GetRasterXSize(); x++) {
					iter.next(index);
					proc.call(band, x, y, var);
					data.set(index, var);
				}				
			}
			if (planes > 1) {
				minPt[2] = 0;
				maxPt[2] = data.dimension(i)-1;
			}
		}
		return data;
	}

	private static DimensionedDataSource<UnsignedInt8Member> loadUByteData(Dataset ds, UnsignedInt8Member var) {
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

	private static DimensionedDataSource<UnsignedInt16Member> loadUShortData(Dataset ds, UnsignedInt16Member var) {
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

	private static DimensionedDataSource<SignedInt16Member> loadShortData(Dataset ds, SignedInt16Member var) {
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

	private static DimensionedDataSource<UnsignedInt32Member> loadUIntData(Dataset ds, UnsignedInt32Member var) {
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

	private static DimensionedDataSource<SignedInt32Member> loadIntData(Dataset ds, SignedInt32Member var) {
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

	private static DimensionedDataSource<Float32Member> loadFloatData(Dataset ds, Float32Member var) {
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

	private static DimensionedDataSource<Float64Member> loadDoubleData(Dataset ds, Float64Member var) {
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

	private static DimensionedDataSource<GaussianInt16Member> loadGaussianShortData(Dataset ds, GaussianInt16Member var) {
		Procedure4<Band,Integer,Integer,GaussianInt16Member> proc =
				new Procedure4<Band, Integer, Integer, GaussianInt16Member>()
		{
			private short[] buffer = new short[2];

			@Override
			public void call(Band band, Integer x, Integer y, GaussianInt16Member outVal) {
				band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), buffer, 0, 0);
				outVal.setR((int) buffer[0]);
				outVal.setI((int) buffer[1]);
			}
		};
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<GaussianInt32Member> loadGaussianIntData(Dataset ds, GaussianInt32Member var) {
		Procedure4<Band,Integer,Integer,GaussianInt32Member> proc =
				new Procedure4<Band, Integer, Integer, GaussianInt32Member>()
		{
			private int[] buffer = new int[2];

			@Override
			public void call(Band band, Integer x, Integer y, GaussianInt32Member outVal) {
				band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), buffer, 0, 0);
				outVal.setR(buffer[0]);
				outVal.setI(buffer[1]);
			}
		};
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<ComplexFloat32Member> loadComplexFloatData(Dataset ds, ComplexFloat32Member var) {
		Procedure4<Band,Integer,Integer,ComplexFloat32Member> proc =
				new Procedure4<Band, Integer, Integer, ComplexFloat32Member>()
		{
			private float[] buffer = new float[2];

			@Override
			public void call(Band band, Integer x, Integer y, ComplexFloat32Member outVal) {
				band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), buffer, 0, 0);
				outVal.setR(buffer[0]);
				outVal.setI(buffer[1]);
			}
		};
		return loadData(ds, var, proc);
	}

	private static DimensionedDataSource<ComplexFloat64Member> loadComplexDoubleData(Dataset ds, ComplexFloat64Member var) {
		Procedure4<Band,Integer,Integer,ComplexFloat64Member> proc =
				new Procedure4<Band, Integer, Integer, ComplexFloat64Member>()
		{
			private double[] buffer = new double[2];

			@Override
			public void call(Band band, Integer x, Integer y, ComplexFloat64Member outVal) {
				band.ReadRaster(x, y, 1, 1, 1, 1, band.getDataType(), buffer, 0, 0);
				outVal.setR(buffer[0]);
				outVal.setI(buffer[1]);
			}
		};
		return loadData(ds, var, proc);
	}
	
}
