<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis maxScale="0" version="3.6.1-Noosa" styleCategories="AllStyleCategories" hasScaleBasedVisibilityFlag="0" minScale="1e+08">
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
    <rasterrenderer type="singlebandpseudocolor" alphaBand="1" classificationMax="10" opacity="1" band="1" classificationMin="-99999">
      <rasterTransparency>
        <singleValuePixelList>
          <pixelListEntry min="-99999" max="-99999" percentTransparent="0"/>
        </singleValuePixelList>
      </rasterTransparency>
      <minMaxOrigin>
        <limits>None</limits>
        <extent>WholeRaster</extent>
        <statAccuracy>Estimated</statAccuracy>
        <cumulativeCutLower>0.02</cumulativeCutLower>
        <cumulativeCutUpper>0.98</cumulativeCutUpper>
        <stdDevFactor>2</stdDevFactor>
      </minMaxOrigin>
      <rastershader>
        <colorrampshader clip="0" colorRampType="INTERPOLATED" classificationMode="1">
          <colorramp type="gradient" name="[source]">
            <prop v="255,255,178,255" k="color1"/>
            <prop v="189,0,38,255" k="color2"/>
            <prop v="0" k="discrete"/>
            <prop v="gradient" k="rampType"/>
            <prop v="0.25;254,204,92,255:0.5;253,141,60,255:0.75;240,59,32,255" k="stops"/>
          </colorramp>
          <item color="#ff00ff" alpha="255" label="-99999" value="-99999"/>
          <item color="#ffffb4" alpha="255" label="0" value="0"/>
          <item color="#ffe281" alpha="255" label="0.1" value="0.1"/>
          <item color="#fec55b" alpha="255" label=".2" value="0.2"/>
          <item color="#fea956" alpha="255" label=".5" value="0.5"/>
          <item color="#fa9059" alpha="255" label="1" value="1"/>
          <item color="#f27a62" alpha="255" label="2" value="2"/>
          <item color="#ed675b" alpha="255" label="5" value="5"/>
          <item color="#e9554d" alpha="255" label="10" value="10"/>
        </colorrampshader>
      </rastershader>
    </rasterrenderer>
    <brightnesscontrast contrast="0" brightness="0"/>
    <huesaturation colorizeRed="255" colorizeGreen="128" colorizeOn="0" colorizeBlue="128" saturation="0" colorizeStrength="100" grayscaleMode="0"/>
    <rasterresampler maxOversampling="2" zoomedInResampler="cubic"/>
  </pipe>
  <blendMode>0</blendMode>
</qgis>
