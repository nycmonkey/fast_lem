// DO NOT EDIT!
// Code generated by ffjson <https://github.com/pquerna/ffjson>
// source: security.go
// DO NOT EDIT!

package fast_lem

import (
	"bytes"
	"fmt"
	fflib "github.com/pquerna/ffjson/fflib/v1"
)

const (
	ffj_t_Requestbase = iota
	ffj_t_Requestno_such_key

	ffj_t_Request_Keys
)

var ffj_key_Request_Keys = []byte("Keys")

func (uj *Request) UnmarshalJSON(input []byte) error {
	fs := fflib.NewFFLexer(input)
	return uj.UnmarshalJSONFFLexer(fs, fflib.FFParse_map_start)
}

func (uj *Request) UnmarshalJSONFFLexer(fs *fflib.FFLexer, state fflib.FFParseState) error {
	var err error = nil
	currentKey := ffj_t_Requestbase
	_ = currentKey
	tok := fflib.FFTok_init
	wantedTok := fflib.FFTok_init

mainparse:
	for {
		tok = fs.Scan()
		//	println(fmt.Sprintf("debug: tok: %v  state: %v", tok, state))
		if tok == fflib.FFTok_error {
			goto tokerror
		}

		switch state {

		case fflib.FFParse_map_start:
			if tok != fflib.FFTok_left_bracket {
				wantedTok = fflib.FFTok_left_bracket
				goto wrongtokenerror
			}
			state = fflib.FFParse_want_key
			continue

		case fflib.FFParse_after_value:
			if tok == fflib.FFTok_comma {
				state = fflib.FFParse_want_key
			} else if tok == fflib.FFTok_right_bracket {
				goto done
			} else {
				wantedTok = fflib.FFTok_comma
				goto wrongtokenerror
			}

		case fflib.FFParse_want_key:
			// json {} ended. goto exit. woo.
			if tok == fflib.FFTok_right_bracket {
				goto done
			}
			if tok != fflib.FFTok_string {
				wantedTok = fflib.FFTok_string
				goto wrongtokenerror
			}

			kn := fs.Output.Bytes()
			if len(kn) <= 0 {
				// "" case. hrm.
				currentKey = ffj_t_Requestno_such_key
				state = fflib.FFParse_want_colon
				goto mainparse
			} else {
				switch kn[0] {

				case 'K':

					if bytes.Equal(ffj_key_Request_Keys, kn) {
						currentKey = ffj_t_Request_Keys
						state = fflib.FFParse_want_colon
						goto mainparse
					}

				}

				if fflib.EqualFoldRight(ffj_key_Request_Keys, kn) {
					currentKey = ffj_t_Request_Keys
					state = fflib.FFParse_want_colon
					goto mainparse
				}

				currentKey = ffj_t_Requestno_such_key
				state = fflib.FFParse_want_colon
				goto mainparse
			}

		case fflib.FFParse_want_colon:
			if tok != fflib.FFTok_colon {
				wantedTok = fflib.FFTok_colon
				goto wrongtokenerror
			}
			state = fflib.FFParse_want_value
			continue
		case fflib.FFParse_want_value:

			if tok == fflib.FFTok_left_brace || tok == fflib.FFTok_left_bracket || tok == fflib.FFTok_integer || tok == fflib.FFTok_double || tok == fflib.FFTok_string || tok == fflib.FFTok_bool || tok == fflib.FFTok_null {
				switch currentKey {

				case ffj_t_Request_Keys:
					goto handle_Keys

				case ffj_t_Requestno_such_key:
					err = fs.SkipField(tok)
					if err != nil {
						return fs.WrapErr(err)
					}
					state = fflib.FFParse_after_value
					goto mainparse
				}
			} else {
				goto wantedvalue
			}
		}
	}

handle_Keys:

	/* handler: uj.Keys type=[]string kind=slice quoted=false*/

	{

		{
			if tok != fflib.FFTok_left_brace && tok != fflib.FFTok_null {
				return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for ", tok))
			}
		}

		if tok == fflib.FFTok_null {
			uj.Keys = nil
		} else {

			uj.Keys = make([]string, 0)

			wantVal := true

			for {

				var tmp_uj__Keys string

				tok = fs.Scan()
				if tok == fflib.FFTok_error {
					goto tokerror
				}
				if tok == fflib.FFTok_right_brace {
					break
				}

				if tok == fflib.FFTok_comma {
					if wantVal == true {
						// TODO(pquerna): this isn't an ideal error message, this handles
						// things like [,,,] as an array value.
						return fs.WrapErr(fmt.Errorf("wanted value token, but got token: %v", tok))
					}
					continue
				} else {
					wantVal = true
				}

				/* handler: tmp_uj__Keys type=string kind=string quoted=false*/

				{

					{
						if tok != fflib.FFTok_string && tok != fflib.FFTok_null {
							return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for string", tok))
						}
					}

					if tok == fflib.FFTok_null {

					} else {

						outBuf := fs.Output.Bytes()

						tmp_uj__Keys = string(string(outBuf))

					}
				}

				uj.Keys = append(uj.Keys, tmp_uj__Keys)
				wantVal = false
			}
		}
	}

	state = fflib.FFParse_after_value
	goto mainparse

wantedvalue:
	return fs.WrapErr(fmt.Errorf("wanted value token, but got token: %v", tok))
wrongtokenerror:
	return fs.WrapErr(fmt.Errorf("ffjson: wanted token: %v, but got token: %v output=%s", wantedTok, tok, fs.Output.String()))
tokerror:
	if fs.BigError != nil {
		return fs.WrapErr(fs.BigError)
	}
	err = fs.Error.ToError()
	if err != nil {
		return fs.WrapErr(err)
	}
	panic("ffjson-generated: unreachable, please report bug.")
done:
	return nil
}

func (mj *Response) MarshalJSON() ([]byte, error) {
	var buf fflib.Buffer
	if mj == nil {
		buf.WriteString("null")
		return buf.Bytes(), nil
	}
	err := mj.MarshalJSONBuf(&buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func (mj *Response) MarshalJSONBuf(buf fflib.EncodingBuffer) error {
	if mj == nil {
		buf.WriteString("null")
		return nil
	}
	var err error
	var obj []byte
	_ = obj
	_ = err
	buf.WriteString(`{"Results":`)
	/* Falling back. type=map[string]*fast_lem.Security kind=map */
	err = buf.Encode(mj.Results)
	if err != nil {
		return err
	}
	buf.WriteByte('}')
	return nil
}

func (mj *Security) MarshalJSON() ([]byte, error) {
	var buf fflib.Buffer
	if mj == nil {
		buf.WriteString("null")
		return buf.Bytes(), nil
	}
	err := mj.MarshalJSONBuf(&buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func (mj *Security) MarshalJSONBuf(buf fflib.EncodingBuffer) error {
	if mj == nil {
		buf.WriteString("null")
		return nil
	}
	var err error
	var obj []byte
	_ = obj
	_ = err
	buf.WriteString(`{ `)
	if len(mj.LegalEntityID) != 0 {
		buf.WriteString(`"LegalEntityId":`)
		fflib.WriteJsonString(buf, string(mj.LegalEntityID))
		buf.WriteByte(',')
	}
	if len(mj.CUSIP) != 0 {
		buf.WriteString(`"Cusip":`)
		fflib.WriteJsonString(buf, string(mj.CUSIP))
		buf.WriteByte(',')
	}
	if len(mj.ISIN) != 0 {
		buf.WriteString(`"ISIN":`)
		fflib.WriteJsonString(buf, string(mj.ISIN))
		buf.WriteByte(',')
	}
	if len(mj.SEDOL) != 0 {
		buf.WriteString(`"Sedol":`)
		fflib.WriteJsonString(buf, string(mj.SEDOL))
		buf.WriteByte(',')
	}
	if len(mj.Ticker) != 0 {
		buf.WriteString(`"Ticker":`)
		fflib.WriteJsonString(buf, string(mj.Ticker))
		buf.WriteByte(',')
	}
	if len(mj.Description) != 0 {
		buf.WriteString(`"Description":`)
		fflib.WriteJsonString(buf, string(mj.Description))
		buf.WriteByte(',')
	}
	buf.Rewind(1)
	buf.WriteByte('}')
	return nil
}

const (
	ffj_t_Securitybase = iota
	ffj_t_Securityno_such_key

	ffj_t_Security_LegalEntityID

	ffj_t_Security_CUSIP

	ffj_t_Security_ISIN

	ffj_t_Security_SEDOL

	ffj_t_Security_Ticker

	ffj_t_Security_Description
)

var ffj_key_Security_LegalEntityID = []byte("LegalEntityId")

var ffj_key_Security_CUSIP = []byte("Cusip")

var ffj_key_Security_ISIN = []byte("ISIN")

var ffj_key_Security_SEDOL = []byte("Sedol")

var ffj_key_Security_Ticker = []byte("Ticker")

var ffj_key_Security_Description = []byte("Description")

func (uj *Security) UnmarshalJSON(input []byte) error {
	fs := fflib.NewFFLexer(input)
	return uj.UnmarshalJSONFFLexer(fs, fflib.FFParse_map_start)
}

func (uj *Security) UnmarshalJSONFFLexer(fs *fflib.FFLexer, state fflib.FFParseState) error {
	var err error = nil
	currentKey := ffj_t_Securitybase
	_ = currentKey
	tok := fflib.FFTok_init
	wantedTok := fflib.FFTok_init

mainparse:
	for {
		tok = fs.Scan()
		//	println(fmt.Sprintf("debug: tok: %v  state: %v", tok, state))
		if tok == fflib.FFTok_error {
			goto tokerror
		}

		switch state {

		case fflib.FFParse_map_start:
			if tok != fflib.FFTok_left_bracket {
				wantedTok = fflib.FFTok_left_bracket
				goto wrongtokenerror
			}
			state = fflib.FFParse_want_key
			continue

		case fflib.FFParse_after_value:
			if tok == fflib.FFTok_comma {
				state = fflib.FFParse_want_key
			} else if tok == fflib.FFTok_right_bracket {
				goto done
			} else {
				wantedTok = fflib.FFTok_comma
				goto wrongtokenerror
			}

		case fflib.FFParse_want_key:
			// json {} ended. goto exit. woo.
			if tok == fflib.FFTok_right_bracket {
				goto done
			}
			if tok != fflib.FFTok_string {
				wantedTok = fflib.FFTok_string
				goto wrongtokenerror
			}

			kn := fs.Output.Bytes()
			if len(kn) <= 0 {
				// "" case. hrm.
				currentKey = ffj_t_Securityno_such_key
				state = fflib.FFParse_want_colon
				goto mainparse
			} else {
				switch kn[0] {

				case 'C':

					if bytes.Equal(ffj_key_Security_CUSIP, kn) {
						currentKey = ffj_t_Security_CUSIP
						state = fflib.FFParse_want_colon
						goto mainparse
					}

				case 'D':

					if bytes.Equal(ffj_key_Security_Description, kn) {
						currentKey = ffj_t_Security_Description
						state = fflib.FFParse_want_colon
						goto mainparse
					}

				case 'I':

					if bytes.Equal(ffj_key_Security_ISIN, kn) {
						currentKey = ffj_t_Security_ISIN
						state = fflib.FFParse_want_colon
						goto mainparse
					}

				case 'L':

					if bytes.Equal(ffj_key_Security_LegalEntityID, kn) {
						currentKey = ffj_t_Security_LegalEntityID
						state = fflib.FFParse_want_colon
						goto mainparse
					}

				case 'S':

					if bytes.Equal(ffj_key_Security_SEDOL, kn) {
						currentKey = ffj_t_Security_SEDOL
						state = fflib.FFParse_want_colon
						goto mainparse
					}

				case 'T':

					if bytes.Equal(ffj_key_Security_Ticker, kn) {
						currentKey = ffj_t_Security_Ticker
						state = fflib.FFParse_want_colon
						goto mainparse
					}

				}

				if fflib.EqualFoldRight(ffj_key_Security_Description, kn) {
					currentKey = ffj_t_Security_Description
					state = fflib.FFParse_want_colon
					goto mainparse
				}

				if fflib.EqualFoldRight(ffj_key_Security_Ticker, kn) {
					currentKey = ffj_t_Security_Ticker
					state = fflib.FFParse_want_colon
					goto mainparse
				}

				if fflib.EqualFoldRight(ffj_key_Security_SEDOL, kn) {
					currentKey = ffj_t_Security_SEDOL
					state = fflib.FFParse_want_colon
					goto mainparse
				}

				if fflib.EqualFoldRight(ffj_key_Security_ISIN, kn) {
					currentKey = ffj_t_Security_ISIN
					state = fflib.FFParse_want_colon
					goto mainparse
				}

				if fflib.EqualFoldRight(ffj_key_Security_CUSIP, kn) {
					currentKey = ffj_t_Security_CUSIP
					state = fflib.FFParse_want_colon
					goto mainparse
				}

				if fflib.SimpleLetterEqualFold(ffj_key_Security_LegalEntityID, kn) {
					currentKey = ffj_t_Security_LegalEntityID
					state = fflib.FFParse_want_colon
					goto mainparse
				}

				currentKey = ffj_t_Securityno_such_key
				state = fflib.FFParse_want_colon
				goto mainparse
			}

		case fflib.FFParse_want_colon:
			if tok != fflib.FFTok_colon {
				wantedTok = fflib.FFTok_colon
				goto wrongtokenerror
			}
			state = fflib.FFParse_want_value
			continue
		case fflib.FFParse_want_value:

			if tok == fflib.FFTok_left_brace || tok == fflib.FFTok_left_bracket || tok == fflib.FFTok_integer || tok == fflib.FFTok_double || tok == fflib.FFTok_string || tok == fflib.FFTok_bool || tok == fflib.FFTok_null {
				switch currentKey {

				case ffj_t_Security_LegalEntityID:
					goto handle_LegalEntityID

				case ffj_t_Security_CUSIP:
					goto handle_CUSIP

				case ffj_t_Security_ISIN:
					goto handle_ISIN

				case ffj_t_Security_SEDOL:
					goto handle_SEDOL

				case ffj_t_Security_Ticker:
					goto handle_Ticker

				case ffj_t_Security_Description:
					goto handle_Description

				case ffj_t_Securityno_such_key:
					err = fs.SkipField(tok)
					if err != nil {
						return fs.WrapErr(err)
					}
					state = fflib.FFParse_after_value
					goto mainparse
				}
			} else {
				goto wantedvalue
			}
		}
	}

handle_LegalEntityID:

	/* handler: uj.LegalEntityID type=string kind=string quoted=false*/

	{

		{
			if tok != fflib.FFTok_string && tok != fflib.FFTok_null {
				return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for string", tok))
			}
		}

		if tok == fflib.FFTok_null {

		} else {

			outBuf := fs.Output.Bytes()

			uj.LegalEntityID = string(string(outBuf))

		}
	}

	state = fflib.FFParse_after_value
	goto mainparse

handle_CUSIP:

	/* handler: uj.CUSIP type=string kind=string quoted=false*/

	{

		{
			if tok != fflib.FFTok_string && tok != fflib.FFTok_null {
				return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for string", tok))
			}
		}

		if tok == fflib.FFTok_null {

		} else {

			outBuf := fs.Output.Bytes()

			uj.CUSIP = string(string(outBuf))

		}
	}

	state = fflib.FFParse_after_value
	goto mainparse

handle_ISIN:

	/* handler: uj.ISIN type=string kind=string quoted=false*/

	{

		{
			if tok != fflib.FFTok_string && tok != fflib.FFTok_null {
				return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for string", tok))
			}
		}

		if tok == fflib.FFTok_null {

		} else {

			outBuf := fs.Output.Bytes()

			uj.ISIN = string(string(outBuf))

		}
	}

	state = fflib.FFParse_after_value
	goto mainparse

handle_SEDOL:

	/* handler: uj.SEDOL type=string kind=string quoted=false*/

	{

		{
			if tok != fflib.FFTok_string && tok != fflib.FFTok_null {
				return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for string", tok))
			}
		}

		if tok == fflib.FFTok_null {

		} else {

			outBuf := fs.Output.Bytes()

			uj.SEDOL = string(string(outBuf))

		}
	}

	state = fflib.FFParse_after_value
	goto mainparse

handle_Ticker:

	/* handler: uj.Ticker type=string kind=string quoted=false*/

	{

		{
			if tok != fflib.FFTok_string && tok != fflib.FFTok_null {
				return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for string", tok))
			}
		}

		if tok == fflib.FFTok_null {

		} else {

			outBuf := fs.Output.Bytes()

			uj.Ticker = string(string(outBuf))

		}
	}

	state = fflib.FFParse_after_value
	goto mainparse

handle_Description:

	/* handler: uj.Description type=string kind=string quoted=false*/

	{

		{
			if tok != fflib.FFTok_string && tok != fflib.FFTok_null {
				return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for string", tok))
			}
		}

		if tok == fflib.FFTok_null {

		} else {

			outBuf := fs.Output.Bytes()

			uj.Description = string(string(outBuf))

		}
	}

	state = fflib.FFParse_after_value
	goto mainparse

wantedvalue:
	return fs.WrapErr(fmt.Errorf("wanted value token, but got token: %v", tok))
wrongtokenerror:
	return fs.WrapErr(fmt.Errorf("ffjson: wanted token: %v, but got token: %v output=%s", wantedTok, tok, fs.Output.String()))
tokerror:
	if fs.BigError != nil {
		return fs.WrapErr(fs.BigError)
	}
	err = fs.Error.ToError()
	if err != nil {
		return fs.WrapErr(err)
	}
	panic("ffjson-generated: unreachable, please report bug.")
done:
	return nil
}
