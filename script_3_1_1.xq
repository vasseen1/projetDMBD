file:write(
  "C:\Users\vasseen1\basex\resultat_3_1.txt",
  string-join(
    for $doc in //document
    let $pmid := $doc/passage/infon[@key="article-id_pmid"]/text()
    let $title := string-join(
      $doc/passage[infon[@key="section_type"]/text()="TITLE"]/text/text(),
      " "
    )
    let $abstract := string-join(
      $doc/passage[
        infon[@key="section_type"]/text()="ABSTRACT" and 
        infon[@key="type"]/text()="abstract"
      ]/text/text(),
      " "
    )
    let $combined := normalize-space(concat($title, " ", $abstract))
    where $pmid != "" and $combined != ""
    return concat($pmid, "/", $combined),
    "&#10;"
  )
)