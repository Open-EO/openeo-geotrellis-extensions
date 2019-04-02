<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0" 
 xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd" 
 xmlns="http://www.opengis.net/sld" 
 xmlns:ogc="http://www.opengis.net/ogc" 
 xmlns:xlink="http://www.w3.org/1999/xlink" 
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <!-- a Named Layer is the basic building block of an SLD document -->
  <NamedLayer>
    <Name>ColorTable_NDVI_V2</Name>
    <UserStyle>
    <!-- Styles can have names, titles and abstracts -->
      <Title>ColorTable_NDVI_V2</Title>
      <Abstract>ColorTable_NDVI_V2</Abstract>
      <!-- FeatureTypeStyles describe how to render different features -->
      <!-- A FeatureTypeStyle for rendering rasters -->
      <FeatureTypeStyle>
        <Rule>
          <Name>ColorTable_NDVI_V2</Name>
          <Title>ColorTable_NDVI_V2</Title>
          <Abstract>ColorTable_NDVI_V2</Abstract>
          <RasterSymbolizer>
            <Opacity>1.0</Opacity>
              <ColorMap >
                <ColorMapEntry color="#8c5c08" quantity="0" label="-0.08"/>              
                <ColorMapEntry color="#8e5f08" quantity="20" label = "0"/>
                <ColorMapEntry color="#c5ad13" quantity="45"  label="0.1" />
                <ColorMapEntry color="#ffff1e" quantity="70" label="0.2"  />
                <ColorMapEntry color="#dae819" quantity="95" label="0.3"  />
                <ColorMapEntry color="#b6d215" quantity="115" label="0.4" />
                <ColorMapEntry color="#91bc11" quantity="145" label="0.5" />
                <ColorMapEntry color="#6da60c" quantity="170" label="0.6"  />
                <ColorMapEntry color="#489008" quantity="195" label="0.7" />
                <ColorMapEntry color="#247a04" quantity="220" label="0.8" />
                <ColorMapEntry color="#006400" quantity="250" label="0.92" />
                <ColorMapEntry color="#dddddd" quantity="251" label="nodata" opacity="1"/>
                <ColorMapEntry color="#dddddd" quantity="253" label="nodata" opacity="1"/>
                <ColorMapEntry color="#dddddd" quantity="254" label="nodata" opacity="0"/>
                <ColorMapEntry color="#dddddd" quantity="255" label="nodata" opacity="0"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>