<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0" 
 xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd" 
 xmlns="http://www.opengis.net/sld" 
 xmlns:ogc="http://www.opengis.net/ogc" 
 xmlns:xlink="http://www.w3.org/1999/xlink" 
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <!-- a Named Layer is the basic building block of an SLD document -->
  <NamedLayer>
    <Name>ColorTable_NDVI_PROBAV</Name>
    <UserStyle>
    <!-- Styles can have names, titles and abstracts -->
      <Title>ColorTable_NDVI_PROBAV</Title>
      <Abstract>ColorTable_NDVI_PROBAV</Abstract>
      <!-- FeatureTypeStyles describe how to render different features -->
      <!-- A FeatureTypeStyle for rendering rasters -->
      <FeatureTypeStyle>
        <Rule>
          <Name>ColorTable_NDVI_PROBAV</Name>
          <Title>ColorTable_NDVI_PROBAV</Title>
          <Abstract>ColorTable_NDVI_PROBAV</Abstract>
          <RasterSymbolizer>
            <Opacity>1.0</Opacity>
            <ChannelSelection>
                <GrayChannel>
                        <SourceChannelName>1</SourceChannelName>
                </GrayChannel>
        	  </ChannelSelection>
              <ColorMap extended="true">
                
                <ColorMapEntry color="#8c5c08" quantity="0" opacity="0"/>            
                <ColorMapEntry color="#8c5c08" quantity="1" />            
                <ColorMapEntry color="#8c5c08" quantity="20" />
                <ColorMapEntry color="#8e5f08" quantity="21" />
                <ColorMapEntry color="#906208" quantity="22" />
                <ColorMapEntry color="#926509" quantity="23" />
                <ColorMapEntry color="#956909" quantity="24" />
                <ColorMapEntry color="#976c0a" quantity="25" />
                <ColorMapEntry color="#996f0a" quantity="26" />
                <ColorMapEntry color="#9c720b" quantity="27" />
                <ColorMapEntry color="#9e760b" quantity="28" />
                <ColorMapEntry color="#a0790b" quantity="29" />
                <ColorMapEntry color="#a37c0c" quantity="30" />
                <ColorMapEntry color="#a57f0c" quantity="31" />
                <ColorMapEntry color="#a7830d" quantity="32" />
                <ColorMapEntry color="#a9860d" quantity="33" />
                <ColorMapEntry color="#ac890e" quantity="34" />
                <ColorMapEntry color="#ae8c0e" quantity="35" />
                <ColorMapEntry color="#b0900f" quantity="36" />
                <ColorMapEntry color="#b3930f" quantity="37" />
                <ColorMapEntry color="#b5960f" quantity="38" />
                <ColorMapEntry color="#b79910" quantity="39" />
                <ColorMapEntry color="#ba9d10" quantity="40" />
                <ColorMapEntry color="#bca011" quantity="41" />
                <ColorMapEntry color="#bea311" quantity="42" />
                <ColorMapEntry color="#c0a612" quantity="43" />
                <ColorMapEntry color="#c3aa12" quantity="44" />
                <ColorMapEntry color="#c5ad13" quantity="45" />
                <ColorMapEntry color="#c7b013" quantity="46" />
                <ColorMapEntry color="#cab413" quantity="47" />
                <ColorMapEntry color="#ccb714" quantity="48" />
                <ColorMapEntry color="#ceba14" quantity="49" />
                <ColorMapEntry color="#d1bd15" quantity="50" />
                <ColorMapEntry color="#d3c115" quantity="51" />
                <ColorMapEntry color="#d5c416" quantity="52" />
                <ColorMapEntry color="#d7c716" quantity="53" />
                <ColorMapEntry color="#daca16" quantity="54" />
                <ColorMapEntry color="#dcce17" quantity="55" />
                <ColorMapEntry color="#ded117" quantity="56" />
                <ColorMapEntry color="#e1d418" quantity="57" />
                <ColorMapEntry color="#e3d718" quantity="58" />
                <ColorMapEntry color="#e5db19" quantity="59" />
                <ColorMapEntry color="#e8de19" quantity="60" />
                <ColorMapEntry color="#eae11a" quantity="61" />
                <ColorMapEntry color="#ece41a" quantity="62" />
                <ColorMapEntry color="#eee81a" quantity="63" />
                <ColorMapEntry color="#f1eb1b" quantity="64" />
                <ColorMapEntry color="#f3ee1b" quantity="65" />
                <ColorMapEntry color="#f5f11c" quantity="66" />
                <ColorMapEntry color="#f8f51c" quantity="67" />
                <ColorMapEntry color="#faf81d" quantity="68" />
                <ColorMapEntry color="#fcfb1d" quantity="69" />
                <ColorMapEntry color="#ffff1e" quantity="70" />
                <ColorMapEntry color="#fdfe1d" quantity="71" />
                <ColorMapEntry color="#fcfd1d" quantity="72" />
                <ColorMapEntry color="#fafc1d" quantity="73" />
                <ColorMapEntry color="#f9fb1d" quantity="74" />
                <ColorMapEntry color="#f7fa1d" quantity="75" />
                <ColorMapEntry color="#f6f91c" quantity="76" />
                <ColorMapEntry color="#f4f81c" quantity="77" />
                <ColorMapEntry color="#f3f71c" quantity="78" />
                <ColorMapEntry color="#f1f71c" quantity="79" />
                <ColorMapEntry color="#f0f61c" quantity="80" />
                <ColorMapEntry color="#eef51c" quantity="81" />
                <ColorMapEntry color="#edf41b" quantity="82" />
                <ColorMapEntry color="#ecf31b" quantity="83" />
                <ColorMapEntry color="#eaf21b" quantity="84" />
                <ColorMapEntry color="#e9f11b" quantity="85" />
                <ColorMapEntry color="#e7f01b" quantity="86" />
                <ColorMapEntry color="#e6ef1b" quantity="87" />
                <ColorMapEntry color="#e4ef1a" quantity="88" />
                <ColorMapEntry color="#e3ee1a" quantity="89" />
                <ColorMapEntry color="#e1ed1a" quantity="90" />
                <ColorMapEntry color="#e0ec1a" quantity="91" />
                <ColorMapEntry color="#deeb1a" quantity="92" />
                <ColorMapEntry color="#ddea1a" quantity="93" />
                <ColorMapEntry color="#dce919" quantity="94" />
                <ColorMapEntry color="#dae819" quantity="95" />
                <ColorMapEntry color="#d9e719" quantity="96" />
                <ColorMapEntry color="#d7e719" quantity="97" />
                <ColorMapEntry color="#d6e619" quantity="98" />
                <ColorMapEntry color="#d4e519" quantity="99" />
                <ColorMapEntry color="#d3e418" quantity="100" />
                <ColorMapEntry color="#d1e318" quantity="101" />
                <ColorMapEntry color="#d0e218" quantity="102" />
                <ColorMapEntry color="#cee118" quantity="103" />
                <ColorMapEntry color="#cde018" quantity="104" />
                <ColorMapEntry color="#cce018" quantity="105" />
                <ColorMapEntry color="#cadf17" quantity="106" />
                <ColorMapEntry color="#c9de17" quantity="107" />
                <ColorMapEntry color="#c7dd17" quantity="108" />
                <ColorMapEntry color="#c6dc17" quantity="109" />
                <ColorMapEntry color="#c4db17" quantity="110" />
                <ColorMapEntry color="#c3da16" quantity="111" />
                <ColorMapEntry color="#c1d916" quantity="112" />
                <ColorMapEntry color="#c0d816" quantity="113" />
                <ColorMapEntry color="#bed816" quantity="114" />
                <ColorMapEntry color="#bdd716" quantity="115" />
                <ColorMapEntry color="#bbd616" quantity="116" />
                <ColorMapEntry color="#bad515" quantity="117" />
                <ColorMapEntry color="#b9d415" quantity="118" />
                <ColorMapEntry color="#b7d315" quantity="119" />
                <ColorMapEntry color="#b6d215" quantity="120" />
                <ColorMapEntry color="#b4d115" quantity="121" />
                <ColorMapEntry color="#b3d015" quantity="122" />
                <ColorMapEntry color="#b1d014" quantity="123" />
                <ColorMapEntry color="#b0cf14" quantity="124" />
                <ColorMapEntry color="#aece14" quantity="125" />
                <ColorMapEntry color="#adcd14" quantity="126" />
                <ColorMapEntry color="#abcc14" quantity="127" />
                <ColorMapEntry color="#aacb14" quantity="128" />
                <ColorMapEntry color="#a9ca13" quantity="129" />
                <ColorMapEntry color="#a7c913" quantity="130" />
                <ColorMapEntry color="#a6c813" quantity="131" />
                <ColorMapEntry color="#a4c813" quantity="132" />
                <ColorMapEntry color="#a3c713" quantity="133" />
                <ColorMapEntry color="#a1c613" quantity="134" />
                <ColorMapEntry color="#a0c512" quantity="135" />
                <ColorMapEntry color="#9ec412" quantity="136" />
                <ColorMapEntry color="#9dc312" quantity="137" />
                <ColorMapEntry color="#9bc212" quantity="138" />
                <ColorMapEntry color="#9ac112" quantity="139" />
                <ColorMapEntry color="#99c112" quantity="140" />
                <ColorMapEntry color="#97c011" quantity="141" />
                <ColorMapEntry color="#96bf11" quantity="142" />
                <ColorMapEntry color="#94be11" quantity="143" />
                <ColorMapEntry color="#93bd11" quantity="144" />
                <ColorMapEntry color="#91bc11" quantity="145" />
                <ColorMapEntry color="#90bb10" quantity="146" />
                <ColorMapEntry color="#8eba10" quantity="147" />
                <ColorMapEntry color="#8db910" quantity="148" />
                <ColorMapEntry color="#8bb910" quantity="149" />
                <ColorMapEntry color="#8ab810" quantity="150" />
                <ColorMapEntry color="#88b710" quantity="151" />
                <ColorMapEntry color="#87b60f" quantity="152" />
                <ColorMapEntry color="#86b50f" quantity="153" />
                <ColorMapEntry color="#84b40f" quantity="154" />
                <ColorMapEntry color="#83b30f" quantity="155" />
                <ColorMapEntry color="#81b20f" quantity="156" />
                <ColorMapEntry color="#80b10f" quantity="157" />
                <ColorMapEntry color="#7eb10e" quantity="158" />
                <ColorMapEntry color="#7db00e" quantity="159" />
                <ColorMapEntry color="#7baf0e" quantity="160" />
                <ColorMapEntry color="#7aae0e" quantity="161" />
                <ColorMapEntry color="#78ad0e" quantity="162" />
                <ColorMapEntry color="#77ac0e" quantity="163" />
                <ColorMapEntry color="#76ab0d" quantity="164" />
                <ColorMapEntry color="#74aa0d" quantity="165" />
                <ColorMapEntry color="#73a90d" quantity="166" />
                <ColorMapEntry color="#71a90d" quantity="167" />
                <ColorMapEntry color="#70a80d" quantity="168" />
                <ColorMapEntry color="#6ea70d" quantity="169" />
                <ColorMapEntry color="#6da60c" quantity="170" />
                <ColorMapEntry color="#6ba50c" quantity="171" />
                <ColorMapEntry color="#6aa40c" quantity="172" />
                <ColorMapEntry color="#68a30c" quantity="173" />
                <ColorMapEntry color="#67a20c" quantity="174" />
                <ColorMapEntry color="#66a20c" quantity="175" />
                <ColorMapEntry color="#64a10b" quantity="176" />
                <ColorMapEntry color="#63a00b" quantity="177" />
                <ColorMapEntry color="#619f0b" quantity="178" />
                <ColorMapEntry color="#609e0b" quantity="179" />
                <ColorMapEntry color="#5e9d0b" quantity="180" />
                <ColorMapEntry color="#5d9c0a" quantity="181" />
                <ColorMapEntry color="#5b9b0a" quantity="182" />
                <ColorMapEntry color="#5a9a0a" quantity="183" />
                <ColorMapEntry color="#589a0a" quantity="184" />
                <ColorMapEntry color="#57990a" quantity="185" />
                <ColorMapEntry color="#55980a" quantity="186" />
                <ColorMapEntry color="#549709" quantity="187" />
                <ColorMapEntry color="#539609" quantity="188" />
                <ColorMapEntry color="#519509" quantity="189" />
                <ColorMapEntry color="#509409" quantity="190" />
                <ColorMapEntry color="#4e9309" quantity="191" />
                <ColorMapEntry color="#4d9209" quantity="192" />
                <ColorMapEntry color="#4b9208" quantity="193" />
                <ColorMapEntry color="#4a9108" quantity="194" />
                <ColorMapEntry color="#489008" quantity="195" />
                <ColorMapEntry color="#478f08" quantity="196" />
                <ColorMapEntry color="#458e08" quantity="197" />
                <ColorMapEntry color="#448d08" quantity="198" />
                <ColorMapEntry color="#438c07" quantity="199" />
                <ColorMapEntry color="#418b07" quantity="200" />
                <ColorMapEntry color="#408a07" quantity="201" />
                <ColorMapEntry color="#3e8a07" quantity="202" />
                <ColorMapEntry color="#3d8907" quantity="203" />
                <ColorMapEntry color="#3b8807" quantity="204" />
                <ColorMapEntry color="#3a8706" quantity="205" />
                <ColorMapEntry color="#388606" quantity="206" />
                <ColorMapEntry color="#378506" quantity="207" />
                <ColorMapEntry color="#358406" quantity="208" />
                <ColorMapEntry color="#348306" quantity="209" />
                <ColorMapEntry color="#338306" quantity="210" />
                <ColorMapEntry color="#318205" quantity="211" />
                <ColorMapEntry color="#308105" quantity="212" />
                <ColorMapEntry color="#2e8005" quantity="213" />
                <ColorMapEntry color="#2d7f05" quantity="214" />
                <ColorMapEntry color="#2b7e05" quantity="215" />
                <ColorMapEntry color="#2a7d04" quantity="216" />
                <ColorMapEntry color="#287c04" quantity="217" />
                <ColorMapEntry color="#277b04" quantity="218" />
                <ColorMapEntry color="#257b04" quantity="219" />
                <ColorMapEntry color="#247a04" quantity="220" />
                <ColorMapEntry color="#227904" quantity="221" />
                <ColorMapEntry color="#217803" quantity="222" />
                <ColorMapEntry color="#207703" quantity="223" />
                <ColorMapEntry color="#1e7603" quantity="224" />
                <ColorMapEntry color="#1d7503" quantity="225" />
                <ColorMapEntry color="#1b7403" quantity="226" />
                <ColorMapEntry color="#1a7303" quantity="227" />
                <ColorMapEntry color="#187302" quantity="228" />
                <ColorMapEntry color="#177202" quantity="229" />
                <ColorMapEntry color="#157102" quantity="230" />
                <ColorMapEntry color="#147002" quantity="231" />
                <ColorMapEntry color="#126f02" quantity="232" />
                <ColorMapEntry color="#116e02" quantity="233" />
                <ColorMapEntry color="#106d01" quantity="234" />
                <ColorMapEntry color="#0e6c01" quantity="235" />
                <ColorMapEntry color="#0d6b01" quantity="236" />
                <ColorMapEntry color="#0b6b01" quantity="237" />
                <ColorMapEntry color="#0a6a01" quantity="238" />
                <ColorMapEntry color="#086901" quantity="239" />
                <ColorMapEntry color="#076800" quantity="240" />
                <ColorMapEntry color="#056700" quantity="241" />
                <ColorMapEntry color="#046600" quantity="242" />
                <ColorMapEntry color="#026500" quantity="243" />
                <ColorMapEntry color="#016400" quantity="244" />
                <ColorMapEntry color="#006400" quantity="245" />
                <ColorMapEntry color="#006400" quantity="250" />
                <ColorMapEntry color="#ffffff" quantity="251" label="nodata" opacity="0"/>                
                <ColorMapEntry color="#ffffff" quantity="255" label="nodata" opacity="0"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
