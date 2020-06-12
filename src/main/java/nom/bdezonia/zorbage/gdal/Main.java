package nom.bdezonia.zorbage.gdal;

import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.ogr.Layer;

public class Main {

	public static void main(String[] args) {
		String filename = "/home/bdz/Downloads/modis.hdf";
	    gdal.AllRegister();
	    Dataset ds = gdal.Open(filename);
		System.out.println(ds.GetDescription());
		System.out.println(" x size " + ds.GetRasterXSize());
		System.out.println(" y size " + ds.GetRasterYSize());
		System.out.println(" layers " + ds.GetLayerCount());
		for (int i = 0; i < ds.GetLayerCount(); i++) {
			Layer layer = ds.GetLayer(i);
			System.out.println("   layer " + i + " " + layer.GetName());
		}
		System.out.println(" rasters " + ds.GetRasterCount());
		for (int i = 0; i < ds.GetRasterCount(); i++) {
			Band band = ds.GetRasterBand(i);
			System.out.println("   band " + i + " " + band.GetRasterDataType() + " " + band.GetDescription());
		}
		Band band = ds.GetRasterBand(0);
		System.out.println(band.getDataType());
		//ds.ReadRaster(xoff, yoff, xsize, ysize, buf_xsize, buf_ysize, buf_type, regularArrayOut, band_list, nPixelSpace)
	}
}
