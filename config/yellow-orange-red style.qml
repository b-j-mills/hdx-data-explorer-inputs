<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis hasScaleBasedVisibilityFlag="0" styleCategories="AllStyleCategories" maxScale="0" minScale="1e+08" version="3.6.1-Noosa">
  <flags>
    <Identifiable>1</Identifiable>
    <Removable>1</Removable>
    <Searchable>1</Searchable>
  </flags>
  <customproperties>
    <property key="WMSBackgroundLayer" value="false"/>
    <property key="WMSPublishDataSourceUrl" value="false"/>
    <property key="embeddedWidgets/count" value="0"/>
    <property key="identify/format" value="Value"/>
  </customproperties>
  <pipe>
    <rasterrenderer band="1" classificationMin="-99999" type="singlebandpseudocolor" opacity="1" classificationMax="10" alphaBand="-1">
      <rasterTransparency/>
      <minMaxOrigin>
        <limits>None</limits>
        <extent>WholeRaster</extent>
        <statAccuracy>Estimated</statAccuracy>
        <cumulativeCutLower>0.02</cumulativeCutLower>
        <cumulativeCutUpper>0.98</cumulativeCutUpper>
        <stdDevFactor>2</stdDevFactor>
      </minMaxOrigin>
      <rastershader>
        <colorrampshader classificationMode="1" colorRampType="INTERPOLATED" clip="0">
          <colorramp type="gradient" name="[source]">
            <prop k="color1" v="255,255,178,255"/>
            <prop k="color2" v="189,0,38,255"/>
            <prop k="discrete" v="0"/>
            <prop k="rampType" v="gradient"/>
            <prop k="stops" v="0.25;254,204,92,255:0.5;253,141,60,255:0.75;240,59,32,255"/>
          </colorramp>
          <item alpha="255" value="-99999" color="#ffffb2" label="-99999"/>
          <item alpha="255" value="0.1" color="#ffe281" label="0.1"/>
          <item alpha="255" value="0.2" color="#fec55b" label=".2"/>
          <item alpha="255" value="0.5" color="#fea956" label=".5"/>
          <item alpha="255" value="1" color="#fa9059" label="1"/>
          <item alpha="255" value="2" color="#f27a62" label="2"/>
          <item alpha="255" value="5" color="#ed675b" label="5"/>
          <item alpha="255" value="10" color="#e9554d" label="10"/>
        </colorrampshader>
      </rastershader>
    </rasterrenderer>
    <brightnesscontrast brightness="0" contrast="0"/>
    <huesaturation colorizeBlue="128" colorizeStrength="100" colorizeGreen="128" grayscaleMode="0" saturation="0" colorizeRed="255" colorizeOn="0"/>
    <rasterresampler maxOversampling="2"/>
  </pipe>
  <blendMode>0</blendMode>
</qgis>
