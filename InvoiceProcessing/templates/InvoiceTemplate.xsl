<?xml version="1.0" encoding="ISO-8859-1"?>
<xs:stylesheet version="1.0"
	xmlns:xs="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format"
	xmlns:java="http://xml.apache.org/xslt/java" exclude-result-prefixes="java">

	<xs:attribute-set name="cell-style">
		<xs:attribute name="border-width">0.5pt</xs:attribute>
		<xs:attribute name="border-style">solid</xs:attribute>
		<xs:attribute name="border-color">black</xs:attribute>
		<xs:attribute name="font-family">sans-serif</xs:attribute>
	</xs:attribute-set>
	<xs:attribute-set name="block-style">
		<xs:attribute name="font-size">  10pt</xs:attribute>
		<xs:attribute name="line-height">15pt</xs:attribute>
		<xs:attribute name="start-indent">1mm</xs:attribute>
		<xs:attribute name="end-indent">  1mm</xs:attribute>
		<xs:attribute name="font-family">sans-serif</xs:attribute>
	</xs:attribute-set>

	<xs:template match="/">
		<fo:root xmlns:fo="http://www.w3.org/1999/XSL/Format">
			<fo:layout-master-set>
				<fo:simple-page-master master-name="DIN-A4"
					page-height="29.7cm" page-width="21cm" margin-top="1cm"
					margin-bottom="2cm" margin-left="2.5cm" margin-right="2.5cm">
					<fo:region-body margin-top="3cm" margin-bottom="3cm" />
					<fo:region-before region-name="header" extent="2.3cm" />
					<fo:region-after region-name="footer" extent="1.5cm" />
				</fo:simple-page-master>
			</fo:layout-master-set>

			<fo:page-sequence master-reference="DIN-A4">
				<fo:static-content flow-name="header">
					<fo:block text-align="right">
						<fo:external-graphic src="./templates/CompanyLogo.gif"
							content-height="2cm" />
					</fo:block>
				</fo:static-content>

				<fo:static-content flow-name="footer">
					<fo:table>
						<fo:table-column column-width="5cm" />
						<fo:table-column column-width="7cm" />
						<fo:table-column column-width="7cm" />
						<fo:table-body font-size="8pt" font-family="sans-serif">
							<fo:table-row>
								<fo:table-cell>
									<fo:block>
										Weltmeister Bank Siegerle
									</fo:block>
									<fo:block>
										IBAN: DE20140713160012018454
									</fo:block>
									<fo:block>
										BIC (SWIFT): WMBADE10120
									</fo:block>
								</fo:table-cell>
								<fo:table-cell>
									<fo:block>
										Vorsitzender des Aufsichtsrates: Max Mustermann
									</fo:block>
									<fo:block>
										Vorstand: Joachim Löw (Vors.), Hans-Dieter Flick,
										Oliver Bierhoff, Andreas Köpke
									</fo:block>
								</fo:table-cell>
								<fo:table-cell>
									<fo:block>
										Amtsgericht Siegerle HRB 12345
									</fo:block>
									<fo:block>
										USt-IdNr.: DE 123456789
									</fo:block>
								</fo:table-cell>
							</fo:table-row>
						</fo:table-body>
					</fo:table>
				</fo:static-content>

				<fo:flow flow-name="xsl-region-body">
					<xs:apply-templates />
					<fo:block id="LastPage" />
				</fo:flow>

			</fo:page-sequence>
		</fo:root>
	</xs:template>

	<xs:template match="Customer">
		<fo:table border-style="none" border-width="0" table-layout="fixed"
			width="100%">
			<fo:table-column column-width="10cm" />
			<fo:table-column column-width="0.2cm" />
			<fo:table-column column-width="3cm" />
			<fo:table-column column-width="0.2cm" />
			<fo:table-column column-width="3cm" />

			<fo:table-body>
				<fo:table-row>
					<fo:table-cell>
						<fo:block xs:use-attribute-sets="block-style" font-size="8pt"
							text-align="left">
							Mustermann AG | Weg der Sterne 4 | D-072014 Siegerle
						</fo:block>
					</fo:table-cell>
				</fo:table-row>
				<fo:table-row>
					<fo:table-cell>
						<fo:block xs:use-attribute-sets="block-style">
							<xs:value-of select="c_first_name" />
							<xs:text> </xs:text>
							<xs:value-of select="c_last_name" />
						</fo:block>
					</fo:table-cell>

					<fo:table-cell>
						<fo:block />
					</fo:table-cell>

					<fo:table-cell>
						<fo:block xs:use-attribute-sets="block-style" text-align="right">
							<xs:text>Rechnungsdatum</xs:text>
						</fo:block>
					</fo:table-cell>

					<fo:table-cell>
						<fo:block />
					</fo:table-cell>

					<fo:table-cell>
						<fo:block xs:use-attribute-sets="block-style">
							<xs:value-of
								select="java:format(java:java.text.SimpleDateFormat.new('dd.MM.yyyy'), java:java.util.Date.new())" />
						</fo:block>
					</fo:table-cell>
				</fo:table-row>

				<fo:table-row>
					<fo:table-cell>
						<fo:block xs:use-attribute-sets="block-style">
							<xs:value-of select="ca_street_name" />
							<xs:text> </xs:text>
							<xs:value-of select="ca_street_type" />
							<xs:text> </xs:text>
							<xs:value-of select="ca_street_number" />
						</fo:block>
					</fo:table-cell>

					<fo:table-cell>
						<fo:block />
					</fo:table-cell>

					<fo:table-cell>
						<fo:block xs:use-attribute-sets="block-style" text-align="right">
							<xs:text>Rechnungs-Nr.</xs:text>
						</fo:block>
					</fo:table-cell>

					<fo:table-cell>
						<fo:block />
					</fo:table-cell>

					<fo:table-cell>
						<fo:block xs:use-attribute-sets="block-style">
							<xs:value-of select="cs_bill_cdemo_sk" />
						</fo:block>
					</fo:table-cell>
				</fo:table-row>
				<fo:table-row>
					<fo:table-cell>
						<fo:block xs:use-attribute-sets="block-style">
							<xs:value-of select="ca_zip" />
							<xs:text> </xs:text>
							<xs:value-of select="ca_county" />
							<xs:text> </xs:text>
							<xs:value-of select="ca_state" />
						</fo:block>
					</fo:table-cell>

					<fo:table-cell>
						<fo:block />
					</fo:table-cell>

					<fo:table-cell>
						<fo:block xs:use-attribute-sets="block-style" text-align="right">
							<xs:text>Kundennummer</xs:text>
						</fo:block>
					</fo:table-cell>

					<fo:table-cell>
						<fo:block />
					</fo:table-cell>

					<fo:table-cell>
						<fo:block xs:use-attribute-sets="block-style">
							<xs:value-of select="c_customer_sk" />
						</fo:block>
					</fo:table-cell>
				</fo:table-row>
				<fo:table-row>
					<fo:table-cell>
						<fo:block xs:use-attribute-sets="block-style">
							<xs:value-of select="ca_country" />
						</fo:block>
					</fo:table-cell>

					<fo:table-cell>
						<fo:block />
					</fo:table-cell>

					<fo:table-cell>
						<fo:block>
							<xs:text> </xs:text>
						</fo:block>
					</fo:table-cell>

					<fo:table-cell>
						<fo:block />
					</fo:table-cell>

					<fo:table-cell>
						<fo:block>
							<xs:text> </xs:text>
						</fo:block>
					</fo:table-cell>
				</fo:table-row>
			</fo:table-body>
		</fo:table>

		<fo:block xs:use-attribute-sets="block-style"
			linefeed-treatment="preserve">
			<xs:text>&#xA;</xs:text>
		</fo:block>

		<fo:block xs:use-attribute-sets="block-style" font-size="14pt"
			font-weight="bold">
			Rechnung
		</fo:block>
		<fo:block xs:use-attribute-sets="block-style">
			Für den Zeitraum 15.11.2014 -
			14.12.2014
		</fo:block>

		<xs:apply-templates select="Positions" />

		<fo:block xs:use-attribute-sets="block-style">
			Zahlungsbedingungen: Zahlbar
			bis zum 15. des Folgemonats.
		</fo:block>

	</xs:template>

	<!-- Tabellenkopf -->
	<xs:template name="table-head">
		<fo:table-row>
			<fo:table-cell xs:use-attribute-sets="cell-style">
				<fo:block xs:use-attribute-sets="block-style" text-align="center">Art.-Nr.
				</fo:block>
			</fo:table-cell>
			<fo:table-cell xs:use-attribute-sets="cell-style">
				<fo:block xs:use-attribute-sets="block-style" text-align="center">Artikel
				</fo:block>
			</fo:table-cell>
			<fo:table-cell xs:use-attribute-sets="cell-style">
				<fo:block xs:use-attribute-sets="block-style" text-align="center">Anzahl
				</fo:block>
			</fo:table-cell>
			<fo:table-cell xs:use-attribute-sets="cell-style">
				<fo:block xs:use-attribute-sets="block-style" text-align="center">Einzelpreis
				</fo:block>
			</fo:table-cell>
			<fo:table-cell xs:use-attribute-sets="cell-style">
				<fo:block xs:use-attribute-sets="block-style" text-align="center">Gesamtpreis
				</fo:block>
			</fo:table-cell>
		</fo:table-row>
	</xs:template>

	<!-- Tabellenkopf -->
	<xs:template name="table-foot">
		<fo:table-row text-align="right">
			<fo:table-cell>
				<fo:block></fo:block>
			</fo:table-cell>
			<fo:table-cell>
				<fo:block></fo:block>
			</fo:table-cell>
			<fo:table-cell number-columns-spanned="2">
				<fo:block xs:use-attribute-sets="block-style">Gesamtpreis
					(netto)
				</fo:block>
			</fo:table-cell>
			<fo:table-cell>
				<fo:block xs:use-attribute-sets="block-style">
					<xs:value-of select="format-number(cs_net_sum, '###,###.00')" />
					<xs:text> &#8364;</xs:text>
				</fo:block>
			</fo:table-cell>
		</fo:table-row>

		<fo:table-row text-align="right">
			<fo:table-cell>
				<fo:block></fo:block>
			</fo:table-cell>
			<fo:table-cell>
				<fo:block></fo:block>
			</fo:table-cell>
			<fo:table-cell number-columns-spanned="2">
				<fo:block xs:use-attribute-sets="block-style">USt.
					(19%)
				</fo:block>
			</fo:table-cell>
			<fo:table-cell>
				<fo:block xs:use-attribute-sets="block-style">
					<xs:value-of select="format-number(cs_tax, '###,###.00')" />
					<xs:text> &#8364;</xs:text>
				</fo:block>
			</fo:table-cell>
		</fo:table-row>

		<fo:table-row border-style="none" text-align="right">
			<fo:table-cell>
				<fo:block></fo:block>
			</fo:table-cell>
			<fo:table-cell>
				<fo:block></fo:block>
			</fo:table-cell>
			<fo:table-cell number-columns-spanned="2">
				<fo:block xs:use-attribute-sets="block-style">Gesamtpreis
					(brutto)
				</fo:block>
			</fo:table-cell>
			<fo:table-cell>
				<fo:block xs:use-attribute-sets="block-style">
					<xs:value-of select="format-number(cs_total_sum, '###,###.00')" />
					<xs:text> &#8364;</xs:text>
				</fo:block>
			</fo:table-cell>
		</fo:table-row>
	</xs:template>

	<!-- Adressen-Root-Element-Template -->
	<xs:template match="Positions">
		<fo:table border-style="none" table-layout="fixed" width="100%">
			<fo:table-column column-width="2cm" />
			<fo:table-column column-width="8cm" />
			<fo:table-column column-width="2cm" />
			<fo:table-column column-width="2cm" />
			<fo:table-column column-width="2.4cm" />
			<fo:table-header>
				<xs:call-template name="table-head" />
			</fo:table-header>
			<fo:table-body>
				<xs:apply-templates select="Position" />
				<xs:call-template name="table-foot" />
			</fo:table-body>
		</fo:table>
	</xs:template>

	<!-- Template der 'adresse'-Elemente -->
	<xs:template match="Position">
		<fo:table-row>
			<fo:table-cell xs:use-attribute-sets="cell-style">
				<fo:block xs:use-attribute-sets="block-style">
					<xs:value-of select="i_item_sk" />
				</fo:block>
			</fo:table-cell>
			<fo:table-cell xs:use-attribute-sets="cell-style">
				<fo:block xs:use-attribute-sets="block-style">
					<xs:text>[</xs:text>
					<xs:value-of select="i_category" />
					<xs:text>/</xs:text>
					<xs:value-of select="i_class" />
					<xs:text>] </xs:text>
					<xs:value-of select="i_brand" />
					<xs:text> </xs:text>
					<xs:value-of select="i_product_name" />
				</fo:block>
			</fo:table-cell>
			<fo:table-cell xs:use-attribute-sets="cell-style"
				text-align="right">
				<fo:block xs:use-attribute-sets="block-style">
					<xs:value-of select="cs_quantity" />
					<xs:text> [</xs:text>
					<xs:value-of select="i_units" />
					<xs:text>]</xs:text>
				</fo:block>
			</fo:table-cell>
			<fo:table-cell xs:use-attribute-sets="cell-style">
				<fo:block xs:use-attribute-sets="block-style" text-align="right">
					<xs:value-of select="format-number(cs_list_price, '###,###.00')" />
					<xs:text> &#8364;</xs:text>
				</fo:block>
			</fo:table-cell>
			<fo:table-cell xs:use-attribute-sets="cell-style">
				<fo:block xs:use-attribute-sets="block-style" text-align="right">
					<xs:value-of select="format-number(cs_ext_list_price, '###,###.00')" />
					<xs:text> &#8364;</xs:text>
				</fo:block>
			</fo:table-cell>
		</fo:table-row>
	</xs:template>

</xs:stylesheet>