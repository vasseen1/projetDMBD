file:write(
  "C:\Users\vasseen1\basex\resultat_3_2.txt",
  string-join(
    for $doc in //document
    let $pmid := $doc/passage/infon[@key="article-id_pmid"]/text()
    let $refs := $doc/passage[infon[@key="section_type"]/text()="REF"]/infon[@key="pub-id_pmid"]/text()
    let $result := concat(
      $pmid,
      if (count($refs) > 0) then concat("/", string-join($refs, "/")) else ""
    )
    where $pmid != ""
    return $result,
    "&#10;"
  )
)