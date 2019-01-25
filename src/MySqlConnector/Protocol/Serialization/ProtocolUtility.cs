using System;
using System.Buffers;
using System.IO;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlConnector.Utilities;

namespace MySqlConnector.Protocol.Serialization
{
	internal static class ProtocolUtility
	{
		public static int GetBytesPerCharacter(CharacterSet characterSet)
		{
			switch (characterSet)
			{
			case CharacterSet.Latin2CzechCaseSensitive:
			case CharacterSet.Dec8SwedishCaseInsensitive:
			case CharacterSet.Cp850GeneralCaseInsensitive:
			case CharacterSet.Latin1German1CaseInsensitive:
			case CharacterSet.Hp8EnglishCaseInsensitive:
			case CharacterSet.Koi8rGeneralCaseInsensitive:
			case CharacterSet.Latin1SwedishCaseInsensitive:
			case CharacterSet.Latin2GeneralCaseInsensitive:
			case CharacterSet.Swe7SwedishCaseInsensitive:
			case CharacterSet.AsciiGeneralCaseInsensitive:
			case CharacterSet.Cp1251BulgarianCaseInsensitive:
			case CharacterSet.Latin1DanishCaseInsensitive:
			case CharacterSet.HebrewGeneralCaseInsensitive:
			case CharacterSet.Tis620ThaiCaseInsensitive:
			case CharacterSet.Latin7EstonianCaseSensitive:
			case CharacterSet.Latin2HungarianCaseInsensitive:
			case CharacterSet.Koi8uGeneralCaseInsensitive:
			case CharacterSet.Cp1251UkrainianCaseInsensitive:
			case CharacterSet.GreekGeneralCaseInsensitive:
			case CharacterSet.Cp1250GeneralCaseInsensitive:
			case CharacterSet.Latin2CroatianCaseInsensitive:
			case CharacterSet.Cp1257LithuanianCaseInsensitive:
			case CharacterSet.Latin5TurkishCaseInsensitive:
			case CharacterSet.Latin1German2CaseInsensitive:
			case CharacterSet.Armscii8GeneralCaseInsensitive:
			case CharacterSet.Cp1250CzechCaseSensitive:
			case CharacterSet.Cp866GeneralCaseInsensitive:
			case CharacterSet.Keybcs2GeneralCaseInsensitive:
			case CharacterSet.MacceGeneralCaseInsensitive:
			case CharacterSet.MacromanGeneralCaseInsensitive:
			case CharacterSet.Cp852GeneralCaseInsensitive:
			case CharacterSet.Latin7GeneralCaseInsensitive:
			case CharacterSet.Latin7GeneralCaseSensitive:
			case CharacterSet.MacceBinary:
			case CharacterSet.Cp1250CroatianCaseInsensitive:
			case CharacterSet.Latin1Binary:
			case CharacterSet.Latin1GeneralCaseInsensitive:
			case CharacterSet.Latin1GeneralCaseSensitive:
			case CharacterSet.Cp1251Binary:
			case CharacterSet.Cp1251GeneralCaseInsensitive:
			case CharacterSet.Cp1251GeneralCaseSensitive:
			case CharacterSet.MacromanBinary:
			case CharacterSet.Cp1256GeneralCaseInsensitive:
			case CharacterSet.Cp1257Binary:
			case CharacterSet.Cp1257GeneralCaseInsensitive:
			case CharacterSet.Binary:
			case CharacterSet.Armscii8Binary:
			case CharacterSet.AsciiBinary:
			case CharacterSet.Cp1250Binary:
			case CharacterSet.Cp1256Binary:
			case CharacterSet.Cp866Binary:
			case CharacterSet.Dec8Binary:
			case CharacterSet.GreekBinary:
			case CharacterSet.HebrewBinary:
			case CharacterSet.Hp8Binary:
			case CharacterSet.Keybcs2Binary:
			case CharacterSet.Koi8rBinary:
			case CharacterSet.Koi8uBinary:
			case CharacterSet.Latin2Binary:
			case CharacterSet.Latin5Binary:
			case CharacterSet.Latin7Binary:
			case CharacterSet.Cp850Binary:
			case CharacterSet.Cp852Binary:
			case CharacterSet.Swe7Binary:
			case CharacterSet.Tis620Binary:
			case CharacterSet.Geostd8GeneralCaseInsensitive:
			case CharacterSet.Geostd8Binary:
			case CharacterSet.Latin1SpanishCaseInsensitive:
			case CharacterSet.Cp1250PolishCaseInsensitive:
			case CharacterSet.Dec8SwedishNoPadCaseInsensitive:
			case CharacterSet.Cp850GeneralNoPadCaseInsensitive:
			case CharacterSet.Hp8EnglishNoPadCaseInsensitive:
			case CharacterSet.Koi8rGeneralNoPadCaseInsensitive:
			case CharacterSet.Latin1SwedishNoPadCaseInsensitive:
			case CharacterSet.Latin2GeneralNoPadCaseInsensitive:
			case CharacterSet.Swe7SwedishNoPadCaseInsensitive:
			case CharacterSet.AsciiGeneralNoPadCaseInsensitive:
			case CharacterSet.HebrewGeneralNoPadCaseInsensitive:
			case CharacterSet.Tis620ThaiNoPadCaseInsensitive:
			case CharacterSet.Koi8uGeneralNoPadCaseInsensitive:
			case CharacterSet.GreekGeneralNoPadCaseInsensitive:
			case CharacterSet.Cp1250GeneralNoPadCaseInsensitive:
			case CharacterSet.Latin5TurkishNoPadCaseInsensitive:
			case CharacterSet.Armscii8GeneralNoPadCaseInsensitive:
			case CharacterSet.Cp866GeneralNoPadCaseInsensitive:
			case CharacterSet.Keybcs2GeneralNoPadCaseInsensitive:
			case CharacterSet.MacCentralEuropeanGeneralNoPadCaseInsensitive:
			case CharacterSet.MacRomanGeneralNoPadCaseInsensitive:
			case CharacterSet.Cp852GeneralNoPadCaseInsensitive:
			case CharacterSet.Latin7GeneralNoPadCaseInsensitive:
			case CharacterSet.MacCentralEuropeanNoPadBinary:
			case CharacterSet.Latin1NoPadBinary:
			case CharacterSet.Cp1251NoPadBinary:
			case CharacterSet.Cp1251GeneralNoPadCaseInsensitive:
			case CharacterSet.MacRomanNoPadBinary:
			case CharacterSet.Cp1256GeneralNoPadCaseInsensitive:
			case CharacterSet.Cp1257NoPadBinary:
			case CharacterSet.Cp1257GeneralNoPadCaseInsensitive:
			case CharacterSet.Armscii8NoPadBinary:
			case CharacterSet.AsciiNoPadBinary:
			case CharacterSet.Cp1250NoPadBinary:
			case CharacterSet.Cp1256NoPadBinary:
			case CharacterSet.Cp866NoPadBinary:
			case CharacterSet.Dec8NoPadBinary:
			case CharacterSet.GreekNoPadBinary:
			case CharacterSet.HebrewNoPadBinary:
			case CharacterSet.Hp8NoPadBinary:
			case CharacterSet.Keybcs2NoPadBinary:
			case CharacterSet.Koi8rNoPadBinary:
			case CharacterSet.Koi8uNoPadBinary:
			case CharacterSet.Latin2NoPadBinary:
			case CharacterSet.Latin5NoPadBinary:
			case CharacterSet.Latin7NoPadBinary:
			case CharacterSet.Cp850NoPadBinary:
			case CharacterSet.Cp852NoPadBinary:
			case CharacterSet.Swe7NoPadBinary:
			case CharacterSet.Tis620NoPadBinary:
			case CharacterSet.Geostd8GeneralNoPadCaseInsensitive:
			case CharacterSet.Geostd8NoPadBinary:
				return 1;

			case CharacterSet.Big5ChineseCaseInsensitive:
			case CharacterSet.SjisJapaneseCaseInsensitive:
			case CharacterSet.EuckrKoreanCaseInsensitive:
			case CharacterSet.Gb2312ChineseCaseInsensitive:
			case CharacterSet.GbkChineseCaseInsensitive:
			case CharacterSet.Ucs2GeneralCaseInsensitive:
			case CharacterSet.Big5Binary:
			case CharacterSet.EuckrBinary:
			case CharacterSet.Gb2312Binary:
			case CharacterSet.GbkBinary:
			case CharacterSet.SjisBinary:
			case CharacterSet.Ucs2Binary:
			case CharacterSet.Cp932JapaneseCaseInsensitive:
			case CharacterSet.Cp932Binary:
			case CharacterSet.Ucs2UnicodeCaseInsensitive:
			case CharacterSet.Ucs2IcelandicCaseInsensitive:
			case CharacterSet.Ucs2LatvianCaseInsensitive:
			case CharacterSet.Ucs2RomanianCaseInsensitive:
			case CharacterSet.Ucs2SlovenianCaseInsensitive:
			case CharacterSet.Ucs2PolishCaseInsensitive:
			case CharacterSet.Ucs2EstonianCaseInsensitive:
			case CharacterSet.Ucs2SpanishCaseInsensitive:
			case CharacterSet.Ucs2SwedishCaseInsensitive:
			case CharacterSet.Ucs2TurkishCaseInsensitive:
			case CharacterSet.Ucs2CzechCaseInsensitive:
			case CharacterSet.Ucs2DanishCaseInsensitive:
			case CharacterSet.Ucs2LithuanianCaseInsensitive:
			case CharacterSet.Ucs2SlovakCaseInsensitive:
			case CharacterSet.Ucs2Spanish2CaseInsensitive:
			case CharacterSet.Ucs2RomanCaseInsensitive:
			case CharacterSet.Ucs2PersianCaseInsensitive:
			case CharacterSet.Ucs2EsperantoCaseInsensitive:
			case CharacterSet.Ucs2HungarianCaseInsensitive:
			case CharacterSet.Ucs2SinhalaCaseInsensitive:
			case CharacterSet.Ucs2German2CaseInsensitive:
			case CharacterSet.Ucs2CroatianCaseInsensitive:
			case CharacterSet.Ucs2Unicode520CaseInsensitive:
			case CharacterSet.Ucs2VietnameseCaseInsensitive:
			case CharacterSet.Ucs2GeneralMySql500CaseInsensitive:
			case CharacterSet.Ucs2CroatianCaseInsensitiveMariaDb:
			case CharacterSet.Ucs2MyanmarCaseInsensitive:
			case CharacterSet.Ucs2ThaiUnicode520Weight2:
			case CharacterSet.Big5ChineseNoPadCaseInsensitive:
			case CharacterSet.SjisJapaneseNoPadCaseInsensitive:
			case CharacterSet.EuckrKoreanNoPadCaseInsensitive:
			case CharacterSet.Gb2312ChineseNoPadCaseInsensitive:
			case CharacterSet.GbkChineseNoPadCaseInsensitive:
			case CharacterSet.Ucs2GeneralNoPadCaseInsensitive:
			case CharacterSet.Big5NoPadBinary:
			case CharacterSet.EuckrNoPadBinary:
			case CharacterSet.Gb2312NoPadBinary:
			case CharacterSet.GbkNoPadBinary:
			case CharacterSet.SjisNoPadBinary:
			case CharacterSet.Ucs2NoPadBinary:
			case CharacterSet.Cp932JapaneseNoPadCaseInsensitive:
			case CharacterSet.Cp932NoPadBinary:
			case CharacterSet.Ucs2UnicodeNoPadCaseInsensitive:
			case CharacterSet.Ucs2Unicode520NoPadCaseInsensitive:
				return 2;

			case CharacterSet.UjisJapaneseCaseInsensitive:
			case CharacterSet.Utf8GeneralCaseInsensitive:
			case CharacterSet.Utf8ToLowerCaseInsensitive:
			case CharacterSet.Utf8Binary:
			case CharacterSet.UjisBinary:
			case CharacterSet.EucjpmsJapaneseCaseInsensitive:
			case CharacterSet.EucjpmsBinary:
			case CharacterSet.Utf8UnicodeCaseInsensitive:
			case CharacterSet.Utf8IcelandicCaseInsensitive:
			case CharacterSet.Utf8LatvianCaseInsensitive:
			case CharacterSet.Utf8RomanianCaseInsensitive:
			case CharacterSet.Utf8SlovenianCaseInsensitive:
			case CharacterSet.Utf8PolishCaseInsensitive:
			case CharacterSet.Utf8EstonianCaseInsensitive:
			case CharacterSet.Utf8SpanishCaseInsensitive:
			case CharacterSet.Utf8SwedishCaseInsensitive:
			case CharacterSet.Utf8TurkishCaseInsensitive:
			case CharacterSet.Utf8CzechCaseInsensitive:
			case CharacterSet.Utf8DanishCaseInsensitive:
			case CharacterSet.Utf8LithuanianCaseInsensitive:
			case CharacterSet.Utf8SlovakCaseInsensitive:
			case CharacterSet.Utf8Spanish2CaseInsensitive:
			case CharacterSet.Utf8RomanCaseInsensitive:
			case CharacterSet.Utf8PersianCaseInsensitive:
			case CharacterSet.Utf8EsperantoCaseInsensitive:
			case CharacterSet.Utf8HungarianCaseInsensitive:
			case CharacterSet.Utf8SinhalaCaseInsensitive:
			case CharacterSet.Utf8German2CaseInsensitive:
			case CharacterSet.Utf8CroatianCaseInsensitive:
			case CharacterSet.Utf8Unicode520CaseInsensitive:
			case CharacterSet.Utf8VietnameseCaseInsensitive:
			case CharacterSet.Utf8GeneralMySql500CaseInsensitive:
			case CharacterSet.Utf8CroatianCaseInsensitiveMariaDb:
			case CharacterSet.Utf8MyanmarCaseInsensitive:
			case CharacterSet.Utf8ThaiUnicode520Weight2:
			case CharacterSet.UjisJapaneseNoPadCaseInsensitive:
			case CharacterSet.Utf8GeneralNoPadCaseInsensitive:
			case CharacterSet.Utf8NoPadBinary:
			case CharacterSet.UjisNoPadBinary:
			case CharacterSet.EucjpmsJapaneseNoPadCaseInsensitive:
			case CharacterSet.EucjpmsNoPadBinary:
			case CharacterSet.Utf8UnicodeNoPadCaseInsensitive:
			case CharacterSet.Utf8Unicode520NoPadCaseInsensitive:
				return 3;

			case CharacterSet.Utf8Mb4GeneralCaseInsensitive:
			case CharacterSet.Utf8Mb4Binary:
			case CharacterSet.Utf16GeneralCaseInsensitive:
			case CharacterSet.Utf16Binary:
			case CharacterSet.Utf16leGeneralCaseInsensitive:
			case CharacterSet.Utf32GeneralCaseInsensitive:
			case CharacterSet.Utf32Binary:
			case CharacterSet.Utf16leBinary:
			case CharacterSet.Utf16UnicodeCaseInsensitive:
			case CharacterSet.Utf16IcelandicCaseInsensitive:
			case CharacterSet.Utf16LatvianCaseInsensitive:
			case CharacterSet.Utf16RomanianCaseInsensitive:
			case CharacterSet.Utf16SlovenianCaseInsensitive:
			case CharacterSet.Utf16PolishCaseInsensitive:
			case CharacterSet.Utf16EstonianCaseInsensitive:
			case CharacterSet.Utf16SpanishCaseInsensitive:
			case CharacterSet.Utf16SwedishCaseInsensitive:
			case CharacterSet.Utf16TurkishCaseInsensitive:
			case CharacterSet.Utf16CzechCaseInsensitive:
			case CharacterSet.Utf16DanishCaseInsensitive:
			case CharacterSet.Utf16LithuanianCaseInsensitive:
			case CharacterSet.Utf16SlovakCaseInsensitive:
			case CharacterSet.Utf16Spanish2CaseInsensitive:
			case CharacterSet.Utf16RomanCaseInsensitive:
			case CharacterSet.Utf16PersianCaseInsensitive:
			case CharacterSet.Utf16EsperantoCaseInsensitive:
			case CharacterSet.Utf16HungarianCaseInsensitive:
			case CharacterSet.Utf16SinhalaCaseInsensitive:
			case CharacterSet.Utf16German2CaseInsensitive:
			case CharacterSet.Utf16CroatianCaseInsensitive:
			case CharacterSet.Utf16Unicode520CaseInsensitive:
			case CharacterSet.Utf16VietnameseCaseInsensitive:
			case CharacterSet.Utf32UnicodeCaseInsensitive:
			case CharacterSet.Utf32IcelandicCaseInsensitive:
			case CharacterSet.Utf32LatvianCaseInsensitive:
			case CharacterSet.Utf32RomanianCaseInsensitive:
			case CharacterSet.Utf32SlovenianCaseInsensitive:
			case CharacterSet.Utf32PolishCaseInsensitive:
			case CharacterSet.Utf32EstonianCaseInsensitive:
			case CharacterSet.Utf32SpanishCaseInsensitive:
			case CharacterSet.Utf32SwedishCaseInsensitive:
			case CharacterSet.Utf32TurkishCaseInsensitive:
			case CharacterSet.Utf32CzechCaseInsensitive:
			case CharacterSet.Utf32DanishCaseInsensitive:
			case CharacterSet.Utf32LithuanianCaseInsensitive:
			case CharacterSet.Utf32SlovakCaseInsensitive:
			case CharacterSet.Utf32Spanish2CaseInsensitive:
			case CharacterSet.Utf32RomanCaseInsensitive:
			case CharacterSet.Utf32PersianCaseInsensitive:
			case CharacterSet.Utf32EsperantoCaseInsensitive:
			case CharacterSet.Utf32HungarianCaseInsensitive:
			case CharacterSet.Utf32SinhalaCaseInsensitive:
			case CharacterSet.Utf32German2CaseInsensitive:
			case CharacterSet.Utf32CroatianCaseInsensitive:
			case CharacterSet.Utf32Unicode520CaseInsensitive:
			case CharacterSet.Utf32VietnameseCaseInsensitive:
			case CharacterSet.Utf8Mb4UnicodeCaseInsensitive:
			case CharacterSet.Utf8Mb4IcelandicCaseInsensitive:
			case CharacterSet.Utf8Mb4LatvianCaseInsensitive:
			case CharacterSet.Utf8Mb4RomanianCaseInsensitive:
			case CharacterSet.Utf8Mb4SlovenianCaseInsensitive:
			case CharacterSet.Utf8Mb4PolishCaseInsensitive:
			case CharacterSet.Utf8Mb4EstonianCaseInsensitive:
			case CharacterSet.Utf8Mb4SpanishCaseInsensitive:
			case CharacterSet.Utf8Mb4SwedishCaseInsensitive:
			case CharacterSet.Utf8Mb4TurkishCaseInsensitive:
			case CharacterSet.Utf8Mb4CzechCaseInsensitive:
			case CharacterSet.Utf8Mb4DanishCaseInsensitive:
			case CharacterSet.Utf8Mb4LithuanianCaseInsensitive:
			case CharacterSet.Utf8Mb4SlovakCaseInsensitive:
			case CharacterSet.Utf8Mb4Spanish2CaseInsensitive:
			case CharacterSet.Utf8Mb4RomanCaseInsensitive:
			case CharacterSet.Utf8Mb4PersianCaseInsensitive:
			case CharacterSet.Utf8Mb4EsperantoCaseInsensitive:
			case CharacterSet.Utf8Mb4HungarianCaseInsensitive:
			case CharacterSet.Utf8Mb4SinhalaCaseInsensitive:
			case CharacterSet.Utf8Mb4German2CaseInsensitive:
			case CharacterSet.Utf8Mb4CroatianCaseInsensitive:
			case CharacterSet.Utf8Mb4Unicode520CaseInsensitive:
			case CharacterSet.Utf8Mb4VietnameseCaseInsensitive:
			case CharacterSet.Gb18030ChineseCaseInsensitive:
			case CharacterSet.Gb18030Binary:
			case CharacterSet.Gb18030Unicode520CaseInsensitive:
			case CharacterSet.Utf8Mb4Uca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4GermanPhonebookUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4IcelandicUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4LatvianUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4RomanianUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4SlovenianUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4PolishUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4EstonianUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4SpanishUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4SwedishUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4TurkishUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4CaseSensitiveUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4DanishUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4LithuanianUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4SlovakUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4TraditionalSpanishUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4LatinUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4EsperantoUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4HungarianUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4CroatianUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4VietnameseUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4Uca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4GermanPhonebookUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4IcelandicUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4LatvianUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4RomanianUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4SlovenianUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4PolishUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4EstonianUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4SpanishUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4SwedishUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4TurkishUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4CaseSensitiveUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4DanishUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4LithuanianUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4SlovakUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4TraditionalSpanishUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4LatinUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4EsperantoUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4HungarianUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4CroatianUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4VietnameseUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4JapaneseUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4JapaneseUca900AccentSensitiveCaseSensitiveKanaSensitive:
			case CharacterSet.Utf8Mb4Uca900AccentSensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4RussianUca900AccentInsensitiveCaseInsensitive:
			case CharacterSet.Utf8Mb4RussianUca900AccentSensitiveCaseSensitive:
			case CharacterSet.Utf8Mb4CroatianCaseInsensitiveMariaDb:
			case CharacterSet.Utf8Mb4MyanmarCaseInsensitive:
			case CharacterSet.Utf8Mb4ThaiUnicode520Weight2:
			case CharacterSet.Utf16CroatianCaseInsensitiveMariaDb:
			case CharacterSet.Utf16MyanmarCaseInsensitive:
			case CharacterSet.Utf16ThaiUnicode520Weight2:
			case CharacterSet.Utf32CroatianCaseInsensitiveMariaDb:
			case CharacterSet.Utf32MyanmarCaseInsensitive:
			case CharacterSet.Utf32ThaiUnicode520Weight2:
			case CharacterSet.Utf8Mb4GeneralNoPadCaseInsensitive:
			case CharacterSet.Utf8Mb4NoPadBinary:
			case CharacterSet.Utf16GeneralNoPadCaseInsensitive:
			case CharacterSet.Utf16NoPadBinary:
			case CharacterSet.Utf16leGeneralNoPadCaseInsensitive:
			case CharacterSet.Utf32GeneralNoPadCaseInsensitive:
			case CharacterSet.Utf32NoPadBinary:
			case CharacterSet.Utf16leNoPadBinary:
			case CharacterSet.Utf16UnicodeNoPadCaseInsensitive:
			case CharacterSet.Utf16Unicode520NoPadCaseInsensitive:
			case CharacterSet.Utf32UnicodeNoPadCaseInsensitive:
			case CharacterSet.Utf32Unicode520NoPadCaseInsensitive:
			case CharacterSet.Utf8Mb4UnicodeNoPadCaseInsensitive:
			case CharacterSet.Utf8Mb4Unicode520NoPadCaseInsensitive:
				return 4;

			default:
				throw new NotSupportedException("Maximum byte length of character set {0} is unknown.".FormatInvariant(characterSet));
			}
		}

		private static ValueTask<Packet> ReadPacketAsync(BufferedByteReader bufferedByteReader, IByteHandler byteHandler, Func<int> getNextSequenceNumber, ProtocolErrorBehavior protocolErrorBehavior, IOBehavior ioBehavior)
		{
			var headerBytesTask = bufferedByteReader.ReadBytesAsync(byteHandler, 4, ioBehavior);
			if (headerBytesTask.IsCompleted)
				return ReadPacketAfterHeader(headerBytesTask.Result, bufferedByteReader, byteHandler, getNextSequenceNumber, protocolErrorBehavior, ioBehavior);
			return AddContinuation(headerBytesTask, bufferedByteReader, byteHandler, getNextSequenceNumber, protocolErrorBehavior, ioBehavior);

			static ValueTask<Packet> AddContinuation(ValueTask<ArraySegment<byte>> headerBytes_, BufferedByteReader bufferedByteReader_, IByteHandler byteHandler_, Func<int> getNextSequenceNumber_, ProtocolErrorBehavior protocolErrorBehavior_, IOBehavior ioBehavior_) =>
				headerBytes_.ContinueWith(x => ReadPacketAfterHeader(x, bufferedByteReader_, byteHandler_, getNextSequenceNumber_, protocolErrorBehavior_, ioBehavior_));
		}

		private static ValueTask<Packet> ReadPacketAfterHeader(ArraySegment<byte> headerBytes, BufferedByteReader bufferedByteReader, IByteHandler byteHandler, Func<int> getNextSequenceNumber, ProtocolErrorBehavior protocolErrorBehavior, IOBehavior ioBehavior)
		{
			if (headerBytes.Count < 4)
			{
				return protocolErrorBehavior == ProtocolErrorBehavior.Throw ?
					ValueTaskExtensions.FromException<Packet>(new EndOfStreamException("Expected to read 4 header bytes but only received {0}.".FormatInvariant(headerBytes.Count))) :
					default(ValueTask<Packet>);
			}

			var payloadLength = (int) SerializationUtility.ReadUInt32(headerBytes.Array, headerBytes.Offset, 3);
			int packetSequenceNumber = headerBytes.Array[headerBytes.Offset + 3];

			var expectedSequenceNumber = getNextSequenceNumber() % 256;
			if (expectedSequenceNumber != -1 && packetSequenceNumber != expectedSequenceNumber)
			{
				if (protocolErrorBehavior == ProtocolErrorBehavior.Ignore)
					return default(ValueTask<Packet>);

				var exception = MySqlProtocolException.CreateForPacketOutOfOrder(expectedSequenceNumber, packetSequenceNumber);
				return ValueTaskExtensions.FromException<Packet>(exception);
			}

			var payloadBytesTask = bufferedByteReader.ReadBytesAsync(byteHandler, payloadLength, ioBehavior);
			if (payloadBytesTask.IsCompleted)
				return CreatePacketFromPayload(payloadBytesTask.Result, payloadLength, protocolErrorBehavior);
			return AddContinuation(payloadBytesTask, payloadLength, protocolErrorBehavior);

			static ValueTask<Packet> AddContinuation(ValueTask<ArraySegment<byte>> payloadBytesTask_, int payloadLength_, ProtocolErrorBehavior protocolErrorBehavior_)
				=> payloadBytesTask_.ContinueWith(x => CreatePacketFromPayload(x, payloadLength_, protocolErrorBehavior_));
		}

		private static ValueTask<Packet> CreatePacketFromPayload(ArraySegment<byte> payloadBytes, int payloadLength, ProtocolErrorBehavior protocolErrorBehavior) =>
			payloadBytes.Count >= payloadLength ? new ValueTask<Packet>(new Packet(payloadBytes)) :
				protocolErrorBehavior == ProtocolErrorBehavior.Throw ? ValueTaskExtensions.FromException<Packet>(new EndOfStreamException("Expected to read {0} payload bytes but only received {1}.".FormatInvariant(payloadLength, payloadBytes.Count))) :
				default(ValueTask<Packet>);

		public static ValueTask<ArraySegment<byte>> ReadPayloadAsync(BufferedByteReader bufferedByteReader, IByteHandler byteHandler, Func<int> getNextSequenceNumber, ArraySegmentHolder<byte> cache, ProtocolErrorBehavior protocolErrorBehavior, IOBehavior ioBehavior)
		{
			cache.Clear();
			return DoReadPayloadAsync(bufferedByteReader, byteHandler, getNextSequenceNumber, cache, protocolErrorBehavior, ioBehavior);
		}

		private static ValueTask<ArraySegment<byte>> DoReadPayloadAsync(BufferedByteReader bufferedByteReader, IByteHandler byteHandler, Func<int> getNextSequenceNumber, ArraySegmentHolder<byte> previousPayloads, ProtocolErrorBehavior protocolErrorBehavior, IOBehavior ioBehavior)
		{
			var readPacketTask = ReadPacketAsync(bufferedByteReader, byteHandler, getNextSequenceNumber, protocolErrorBehavior, ioBehavior);
			while (readPacketTask.IsCompleted)
			{
				if (HasReadPayload(previousPayloads, readPacketTask.Result, protocolErrorBehavior, out var result))
					return result;

				readPacketTask = ReadPacketAsync(bufferedByteReader, byteHandler, getNextSequenceNumber, protocolErrorBehavior, ioBehavior);
			}

			return AddContinuation(readPacketTask, bufferedByteReader, byteHandler, getNextSequenceNumber, previousPayloads, protocolErrorBehavior, ioBehavior);

			static ValueTask<ArraySegment<byte>> AddContinuation(ValueTask<Packet> readPacketTask_, BufferedByteReader bufferedByteReader_, IByteHandler byteHandler_, Func<int> getNextSequenceNumber_, ArraySegmentHolder<byte> previousPayloads_, ProtocolErrorBehavior protocolErrorBehavior_, IOBehavior ioBehavior_)
			{
				return readPacketTask_.ContinueWith(packet =>
					HasReadPayload(previousPayloads_, packet, protocolErrorBehavior_, out var result_) ? result_ :
						DoReadPayloadAsync(bufferedByteReader_, byteHandler_, getNextSequenceNumber_, previousPayloads_, protocolErrorBehavior_, ioBehavior_));
			}
		}

		private static bool HasReadPayload(ArraySegmentHolder<byte> previousPayloads, Packet packet, ProtocolErrorBehavior protocolErrorBehavior, out ValueTask<ArraySegment<byte>> result)
		{
			if (previousPayloads.Count == 0 && packet.Contents.Count < MaxPacketSize)
			{
				result = new ValueTask<ArraySegment<byte>>(packet.Contents);
				return true;
			}

			var previousPayloadsArray = previousPayloads.Array;
			if (previousPayloadsArray == null)
				previousPayloadsArray = new byte[ProtocolUtility.MaxPacketSize + 1];
			else if (previousPayloads.Offset + previousPayloads.Count + packet.Contents.Count > previousPayloadsArray.Length)
				Array.Resize(ref previousPayloadsArray, previousPayloadsArray.Length * 2);

			Buffer.BlockCopy(packet.Contents.Array, packet.Contents.Offset, previousPayloadsArray, previousPayloads.Offset + previousPayloads.Count, packet.Contents.Count);
			previousPayloads.ArraySegment = new ArraySegment<byte>(previousPayloadsArray, previousPayloads.Offset, previousPayloads.Count + packet.Contents.Count);

			if (packet.Contents.Count < ProtocolUtility.MaxPacketSize)
			{
				result = new ValueTask<ArraySegment<byte>>(previousPayloads.ArraySegment);
				return true;
			}

			result = default(ValueTask<ArraySegment<byte>>);
			return false;
		}

		public static ValueTask<int> WritePayloadAsync(IByteHandler byteHandler, Func<int> getNextSequenceNumber, ArraySegment<byte> payload, IOBehavior ioBehavior)
		{
			return payload.Count <= MaxPacketSize ? WritePacketAsync(byteHandler, getNextSequenceNumber(), payload, ioBehavior) :
				CreateTask(byteHandler, getNextSequenceNumber, payload, ioBehavior);

			static ValueTask<int> CreateTask(IByteHandler byteHandler_, Func<int> getNextSequenceNumber_, ArraySegment<byte> payload_, IOBehavior ioBehavior_)
			{
				var writeTask = default(ValueTask<int>);
				for (var bytesSent = 0; bytesSent < payload_.Count; bytesSent += MaxPacketSize)
				{
					var contents = new ArraySegment<byte>(payload_.Array, payload_.Offset + bytesSent, Math.Min(MaxPacketSize, payload_.Count - bytesSent));
					writeTask = writeTask.ContinueWith(x => WritePacketAsync(byteHandler_, getNextSequenceNumber_(), contents, ioBehavior_));
				}
				return writeTask;
			}
		}

		private static ValueTask<int> WritePacketAsync(IByteHandler byteHandler, int sequenceNumber, ArraySegment<byte> contents, IOBehavior ioBehavior)
		{
			var bufferLength = contents.Count + 4;
			var buffer = ArrayPool<byte>.Shared.Rent(bufferLength);
			SerializationUtility.WriteUInt32((uint) contents.Count, buffer, 0, 3);
			buffer[3] = (byte) sequenceNumber;
			Buffer.BlockCopy(contents.Array, contents.Offset, buffer, 4, contents.Count);
			var task = byteHandler.WriteBytesAsync(new ArraySegment<byte>(buffer, 0, bufferLength), ioBehavior);
			if (task.IsCompletedSuccessfully)
			{
				ArrayPool<byte>.Shared.Return(buffer);
				return default(ValueTask<int>);
			}
			return AddContinuation(task, buffer);

			static ValueTask<int> AddContinuation(ValueTask<int> task_, byte[] buffer_)
			{
				return task_.ContinueWith(x =>
				{
					ArrayPool<byte>.Shared.Return(buffer_);
					return default(ValueTask<int>);
				});
			}
		}

		public const int MaxPacketSize = 16777215;
	}
}
