/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.mif.manager.capaoracle;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.mif.manager.interficiepersistencia.GestorBDManagerException;
import org.mif.manager.interficiepersistencia.IGestorBDManager;
import org.mif.manager.model.Categoria;
import org.mif.manager.model.Equip;
import org.mif.manager.model.Jugador;
import org.mif.manager.model.Membre;
import org.mif.manager.model.Temporada;

/**
 *
 * @author markos
 */
public class GestorBDManagerJdbc implements IGestorBDManager
{
    /*
     * Aquest objecte és el que ha de mantenir la connexió amb el SGBD Es crea
     * en el constructor d'aquesta classe i manté la connexió fins que el
     * programador decideix tancar la connexió amb el mètode tancarCapa
     */
    private Connection conn;

    /**
     * Sentències preparades que es crearan només 1 vegada i s'utilitzaran
     * tantes vegades com siguin necessàries. En aquest programa es creen la
     * primera vegada que es necessiten i encara no han estat creades. També es
     * podrien crear al principi del programa, una vegada establerta la
     * connexió.
     */
    // Statements de temporada
    private PreparedStatement psInsTemp;
    // Statements d'equip
    private PreparedStatement psSelListEquipsTemporada;
    private PreparedStatement psSelListEquip;
    private PreparedStatement psSelEquipTitular;
    private PreparedStatement psSelEq;
    private PreparedStatement psInsEq;
    private PreparedStatement psModEq;
    private PreparedStatement psDelEq;
    // Statements de jugador
    private PreparedStatement psSelListJugadorEquip;
    private PreparedStatement psSelListJugadorEquipInscriptibles;
    private PreparedStatement psSelListJugador;
    private PreparedStatement psSelJug;
    private PreparedStatement psSelTitular;
    private PreparedStatement psInsJug;
    private PreparedStatement psModJug;
    private PreparedStatement psDelJug;
    // Statements de membre
    private PreparedStatement psInsMem;
    private PreparedStatement psModMem;
    private PreparedStatement psDelMem;
    private PreparedStatement psDelMembsEq;
    // Statements de categoria
    private PreparedStatement psSelCat;
    // Statements d'usuari
    private PreparedStatement psRetUsu;
    
    /**
     * Intenta connectar amb la BD utilitzant les credencials de l'arxiu XML
     * 
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException
     */
    public GestorBDManagerJdbc() throws GestorBDManagerException
    {
        String nomFitxer = "managerJDBC.xml";
        
        try 
        {
            Properties props = new Properties();
            props.loadFromXML(new FileInputStream(nomFitxer));
            String[] claus = {"url", "user", "password"};
            String[] valors = new String[3];
            for (int i = 0; i < claus.length; i++)
            {
                valors[i] = props.getProperty(claus[i]);
                if (valors[i] == null || valors[i].isEmpty())
                {
                    throw new GestorBDManagerException("L'arxiu " + nomFitxer + " no troba la clau " + claus[i]);
                }
            }
            conn = DriverManager.getConnection(valors[0], valors[1], valors[2]);
            conn.setAutoCommit(false);
        }
        catch (IOException ex)
        {
            throw new GestorBDManagerException("No s'ha pogut recuperar correctament l'arxiu de configuració " +nomFitxer+ "\n" + ex.getMessage());
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("No es pot establir la connexió.\n"+ ex.getMessage());
        }
    }
    
    /**
     * Retorna el llistat de temporades de la BD
     * 
     * @return Llista de temporades recuperades
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public List<Temporada> obtenirTemporades() throws GestorBDManagerException
    {
        List<Temporada> temps = new ArrayList<>();
        Statement q = null;
        try
        {
            q = conn.createStatement();
            ResultSet rs = q.executeQuery("SELECT anny FROM temporada");
            while (rs.next())
            {
                LocalDate anny = rs.getDate("anny").toLocalDate();
                Temporada t = new Temporada(anny);
                temps.add(t);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en intentar recuperar la llista de temporades.\n"+ ex.getMessage());
        }
        finally
        {
            if (q != null)
            {
                try
                {
                    q.close();
                }
                catch (SQLException ex)
                {
                    throw new GestorBDManagerException("Error en intentar tancar la sentència que ha recuperat la llista de temporades.\n"+ ex.getMessage());
                }
            }
        }
        return temps;
    }
    
    /**
     * Crea una nova temporada a la BD
     * 
     * @param data Data de la nova temporada en string
     * @return int indicant si s'ha afegit la temporada (1) o no (0)
     * @throws GestorBDManagerException 
     */
    @Override
    public int crearTemporada(String data) throws GestorBDManagerException
    {
        int inserit = 0;
        int any = Integer.parseInt(data);
        
        LocalDate temp = LocalDate.of(any, 1, 1);
        
        if (psInsTemp == null)
        {
            try
            {
                psInsTemp = conn.prepareStatement("INSERT INTO temporada (anny) VALUES (?)");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psInsTemp\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psInsTemp.setDate(1, Date.valueOf(temp));
            inserit = psInsTemp.executeUpdate();

        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en inserir la temporada\n" + ex.getMessage());
        }
        
        return inserit;
    }
    
    /**
     * Retorna el llistat de tots els equips de la BD segons temporada escollida
     * 
     * @param t Temporada escollida
     * @return Llista d'equips recuperats
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException
     */
    @Override
    public List<Equip> obtenirEquips(Temporada t) throws GestorBDManagerException
    {
        List<Equip> equips = new ArrayList<>();
        
        if (psSelListEquipsTemporada == null)
        {
            try
            {
                psSelListEquipsTemporada = conn.prepareStatement("SELECT id, nom, tipus, nom_cat, temporada FROM equip WHERE temporada = ?");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psSelListEquip:\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psSelListEquipsTemporada.setDate(1, Date.valueOf(t.getAnny()));
            ResultSet rs = psSelListEquipsTemporada.executeQuery();
            while (rs.next())
            {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                String tipus = rs.getString("tipus");
                String nom_cat = rs.getString("nom_cat");
                LocalDate anny = rs.getDate("temporada").toLocalDate();
                
                Equip e = new Equip();
                e.setId(id);
                e.setNom(nom);
                e.setTipus(tipus);
                e.setCategoria(obtenirCategoria(nom_cat));
                e.setTemporada(anny);
                equips.add(e);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en intentar recuperar la llista d'equips\n"+ ex.getMessage());
        }
        
        return equips;
    }
    
    /**
     * Retorna un llistat amb els equips que tinguin la String introduida al nom i/o siguin del tipus introduït
     * 
     * @param nomEquip Nom de l'equip que es vol cercar
     * @param tipusEquip Tipus de l'equip que es vol cercar
     * @return Llistat d'equips que concordin amb nom i/o tipus
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public List<Equip> buscarEquips(String nomEquip, String tipusEquip) throws GestorBDManagerException
    {
        List<Equip> equips = new ArrayList<>();
        
        if (psSelListEquip == null)
        {
            try
            {
                psSelListEquip = conn.prepareStatement("SELECT id, nom, tipus, nom_cat, temporada FROM equip WHERE nom LIKE ? AND tipus = ?");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psSelListEquip:\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psSelListEquip.setString(1, "%"+nomEquip+"%");
            psSelListEquip.setString(2, tipusEquip);
            ResultSet rs = psSelListEquip.executeQuery();
            while (rs.next())
            {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                String tipus = rs.getString("tipus");
                String nom_cat = rs.getString("nom_cat");
                LocalDate anny = rs.getDate("temporada").toLocalDate();
                
                Equip e = new Equip();
                e.setId(id);
                e.setNom(nom);
                e.setTipus(tipus);
                e.setCategoria(obtenirCategoria(nom_cat));
                e.setTemporada(anny);
                equips.add(e);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en seleccionar els equips de nom i/o tipus indicat:\n"+ ex.getMessage());
        }
        
        return equips;
    }
    
    /**
     * Retorna l'id de l'equip on el jugador és titular, o null si no n'és a cap.
     * 
     * @param jugadorId id del jugador a consultar
     * @return id de l'equip o null si no n'és titular a cap equip
     * @throws GestorBDManagerException 
    */
    @Override
    public Integer obtenirEquipOnEsTitular(int jugadorId) throws GestorBDManagerException
    {
        Integer equipId = null;

        if (psSelEquipTitular == null)
        {
            try
            {
                psSelEquipTitular = conn.prepareStatement(
                    "SELECT equip FROM membre WHERE jugador = ? AND titular = 'Titular'"
                );
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psSelEquipTitular:\n"+ ex.getMessage());
            }
        }

        ResultSet rs = null;
        try
        {
            psSelEquipTitular.setInt(1, jugadorId);
            rs = psSelEquipTitular.executeQuery();
            if (rs.next())
            {
                equipId = rs.getInt("equip");
            }
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en seleccionar l'equip on el jugador és titular:\n"+ ex.getMessage());
        }
        finally
        {
            if (rs != null)
            {
                try { rs.close(); } catch (SQLException ex) { /* Ignora */ }
            }
        }

        return equipId;
    }


    /**
     * Retorna l'equip obtingut a partir de la seva id
     * 
     * @param idEquip id de l'equip a obtenir
     * @return Equip obtingut
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public Equip obtenirEquip(int idEquip) throws GestorBDManagerException
    {
        Equip equip = new Equip();
        
        if (psSelEq == null)
        {
            try
            {
                psSelEq = conn.prepareStatement("SELECT id, nom, tipus, nom_cat, temporada FROM equip WHERE id = ?");
            }
            catch(SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psSelEq:\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psSelEq.setInt(1, idEquip);
            ResultSet rs = psSelEq.executeQuery();
            while (rs.next())
            {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                String tipus = rs.getString("tipus");
                String nom_cat = rs.getString("nom_cat");
                LocalDate anny = rs.getDate("temporada").toLocalDate();
                equip.setId(id);
                equip.setNom(nom);
                equip.setTipus(tipus);
                equip.setCategoria(obtenirCategoria(nom_cat));
                equip.setTemporada(anny);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en seleccionar l'equip amb id indicat:\n"+ ex.getMessage());
        }
        
        return equip;
    }
    
    /**
     * Insereix l'equip a la BD
     * 
     * @param e Equip a inserir
     * @return int Indicant la cuantitat de linies afectades a la BD (1 o 0)
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public int afegirEquip(Equip e) throws GestorBDManagerException
    {
        int inserit = 0;
        
        if (psInsEq == null)
        {
            try
            {
                psInsEq = conn.prepareStatement("INSERT INTO equip (nom, tipus, nom_cat, temporada) VALUES (?,?,?,?)");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psInsEq\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psInsEq.setString(1, e.getNom());
            switch (e.getTipus())
            {
                case "Femení":
                    psInsEq.setString(2, "D");
                    break;
                    
                case "Mixte":
                    psInsEq.setString(2, "M");
                    break;
                    
                case "Masculí":
                    psInsEq.setString(2, "H");
                    break;
            }
            psInsEq.setString(3, e.getCategoria().getNom());
            psInsEq.setDate(4, Date.valueOf(e.getTemporada()));
            System.out.println(e.getTemporada());
            inserit = psInsEq.executeUpdate();

        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en inserir l'equip:\n"+ ex.getMessage());
        }
        
        return inserit;
    }
    
    /**
     * Modifica l'equip a la BD
     * 
     * @param e Equip a modificar
     * @return  int Indicant la cuantitat de linies afectades a la BD (1 o 0)
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public int modificarEquip(Equip e) throws GestorBDManagerException
    {
        int modificat = 0;
        
        if (psModEq == null)
        {
            try {
                psModEq = conn.prepareStatement("UPDATE equip SET "
                                              + "nom = ?, tipus = ?, nom_cat = ?, temporada = ? "
                                              + "WHERE id = ?");
            } catch (SQLException ex) {
                throw new GestorBDManagerException("Error en preparar sentència psModEq:\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psModEq.setString(1, e.getNom());
            psModEq.setString(2, e.getTipus());
            psModEq.setString(3, e.getCategoria().getNom());
            psModEq.setDate(4, Date.valueOf(e.getTemporada()));
            modificat = psModEq.executeUpdate();

        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en modificar l'equip:\n"+ ex.getMessage());
        }
        
        return modificat;
    }
    
    /**
     * Esborra l'equip de la BD
     * 
     * @param e Equip a eliminar
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public int eliminarEquip(Equip e) throws GestorBDManagerException
    {
        int eliminat = 0;
        
        if (psDelEq == null)
        {
            try
            {
                psDelEq = conn.prepareStatement("DELETE FROM equip WHERE id = ?");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psDelEq:\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psDelEq.setInt(1, e.getId());
            eliminat = psDelEq.executeUpdate();

        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en eliminar l'equip:\n"+ ex.getMessage());
        }
        
        return eliminat;
    }
    
    /**
     * Retorna tots els jugadors de la BD
     * 
     * @return Llista de tots els jugadors
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public List<Jugador> obtenirJugadors() throws GestorBDManagerException
    {
        List<Jugador> jugadors = new ArrayList<>();
        Statement q = null;
        try
        {
            q = conn.createStatement();
            ResultSet rs = q.executeQuery("SELECT * FROM jugador");
            while (rs.next())
            {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                String cognoms = rs.getString("cognoms");
                String sexe = rs.getString("sexe");
                LocalDate data_naix = rs.getDate("data_naix").toLocalDate();
                String idlegal = rs.getString("idlegal");
                String iban = rs.getString("iban");
                LocalDate fi_revi = rs.getDate("any_fi_revi_medica").toLocalDate();
                String adresa = rs.getString("adresa");
                String foto = rs.getString("foto");
                
                Jugador j = new Jugador();
                j.setId(id);
                j.setNom(nom);
                j.setCognoms(cognoms);
                j.setSexe(sexe);
                j.setData_naix(data_naix);
                j.setIdLegal(idlegal);
                j.setIban(iban);
                j.setAny_fi_revi_medica(fi_revi);
                j.setAdresa(adresa);
                j.setFoto(foto);

                jugadors.add(j);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en intentar recuperar la llista de jugadors\n"+ ex.getMessage());
        }
        finally
        {
            if (q != null)
            {
                try
                {
                    q.close();
                }
                catch (SQLException ex)
                {
                    throw new GestorBDManagerException("Error en intentar tancar la sentència que ha recuperat la llista de jugadors.\n"+ ex.getMessage());
                }
            }
        }
        
        return jugadors;
    }
    
    /**
     * Retorna els jugadors d'un equip
     * 
     * @param eqId EquipId del qual volem obtenir els jugadors
     * @return Llista dels jugadors de l'equip pasat per parametre
     * @throws GestorBDManagerException 
     */
    @Override
    public List<Jugador> obtenirJugadorsEquip(int eqId) throws GestorBDManagerException
    {
        List<Jugador> jugadors = new ArrayList<>();
        
        if (psSelListJugadorEquip == null)
        {
            try
            {
                psSelListJugadorEquip = conn.prepareStatement("SELECT j.* FROM jugador j " +
                                                              "INNER JOIN membre m ON m.jugador = j.id " +
                                                              "INNER JOIN equip e ON e.id = m.equip " +
                                                              "WHERE e.id = ?");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psSelListJugadorEquip:\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psSelListJugadorEquip.setInt(1, eqId);
            ResultSet rs = psSelListJugadorEquip.executeQuery();
            while (rs.next())
            {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                String cognoms = rs.getString("cognoms");
                String sexe = rs.getString("sexe");
                LocalDate data_naix = rs.getDate("data_naix").toLocalDate();
                String idlegal = rs.getString("idlegal");
                String iban = rs.getString("iban");
                LocalDate fi_revi = rs.getDate("any_fi_revi_medica").toLocalDate();
                String adresa = rs.getString("adresa");
                String foto = rs.getString("foto");
                
                Jugador j = new Jugador();
                j.setId(id);
                j.setNom(nom);
                j.setCognoms(cognoms);
                j.setSexe(sexe);
                j.setData_naix(data_naix);
                j.setIdLegal(idlegal);
                j.setIban(iban);
                j.setAny_fi_revi_medica(fi_revi);
                j.setAdresa(adresa);
                j.setFoto(foto);
                
                jugadors.add(j);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en seleccionar els jugadors de l'equip:\n"+ ex.getMessage());
        }
        
        return jugadors;
    }
    
    /**
     * Retorna els jugadors inscriptibles dins l'equip especificat
     * 
     * @param eqId EquipId del qual volem obtenir els jugadors inscriptibles
     * @return Llista dels jugadors que podrien inscriure-s'hi a l'equip
     * @throws GestorBDManagerException 
     */
    @Override
    public List<Jugador> obtenirJugadorsEquipInscriptibles(int eqId) throws GestorBDManagerException
    {
        List<Jugador> jugadors = new ArrayList<>();
        
        if (psSelListJugadorEquipInscriptibles == null)
        {
            try
            {
                // La funció extract de sql l'he trobat aqui: https://www.sqltutorial.org/sql-date-functions/how-to-extract-year-from-date-in-sql/
                psSelListJugadorEquipInscriptibles = conn.prepareStatement("SELECT j.*" +
                                                                           "FROM jugador j " +
                                                                           "INNER JOIN equip e on e.id = ? " +
                                                                           "INNER JOIN categoria c ON c.nom = e.nom_cat " +
                                                                           "WHERE ( (e.tipus = 'M') OR (e.tipus <> 'M' AND e.tipus = j.sexe) )" +
                                                                           "AND (EXTRACT(YEAR FROM e.temporada) - EXTRACT(YEAR FROM j.data_naix) <= COALESCE(c.edat_max, 99))\n" +
                                                                           "AND j.id NOT IN ( " +
                                                                                "SELECT j2.id " +
                                                                                "FROM jugador j2 " +
                                                                                "INNER JOIN membre m ON m.jugador = j2.id " +
                                                                                "INNER JOIN equip e2 ON m.equip = e2.id " +
                                                                                "WHERE e2.id = ?)");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psSelListJugadorEquipInscriptibles:\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psSelListJugadorEquipInscriptibles.setInt(1, eqId);
            psSelListJugadorEquipInscriptibles.setInt(2, eqId);
            ResultSet rs = psSelListJugadorEquipInscriptibles.executeQuery();
            while (rs.next())
            {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                String cognoms = rs.getString("cognoms");
                String sexe = rs.getString("sexe");
                LocalDate data_naix = rs.getDate("data_naix").toLocalDate();
                String idlegal = rs.getString("idlegal");
                String iban = rs.getString("iban");
                LocalDate fi_revi = rs.getDate("any_fi_revi_medica").toLocalDate();
                String adresa = rs.getString("adresa");
                String foto = rs.getString("foto");
                
                Jugador j = new Jugador();
                j.setId(id);
                j.setNom(nom);
                j.setCognoms(cognoms);
                j.setSexe(sexe);
                j.setData_naix(data_naix);
                j.setIdLegal(idlegal);
                j.setIban(iban);
                j.setAny_fi_revi_medica(fi_revi);
                j.setAdresa(adresa);
                j.setFoto(foto);
                
                jugadors.add(j);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en seleccionar els jugadors de l'equip:\n"+ ex.getMessage());
        }
        
        return jugadors;
    }
    
    /**
     * Retorna llistat amb els jugadors que concordin amb nom/cognoms, equip o sexe introduits per cercar
     * 
     * @param nomJugador Nom i/o Cognoms dels jugadors o jugador que es vol cercar
     * @param e Equip del qual es volen obtenir els o el jugador
     * @param sexe_busqueda Sexe del jugador o jugadors que es volen cercar
     * @return Llistat amb els jugadors (o jugador) que compleixin les condicions introduides
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public List<Jugador> buscarJugadors(String nomJugador, Equip e, String sexe_busqueda) throws GestorBDManagerException
    {
        List<Jugador> jugadors = new ArrayList<>();
        
        if (psSelListJugador == null)
        {
            try
            {   // funció convert l'he tret d'aqui: https://stackoverflow.com/questions/35689157/compare-strings-ignoring-accents-in-sql-oracle per poder ignorar accents
                psSelListJugador = conn.prepareStatement("SELECT j.* FROM jugador j " +
                                                        "INNER join membre m ON m.jugador = j.id " +
                                                        "INNER join equip e ON e.id = m.equip " +
                                                        "WHERE e.id = ? " +
                                                        "AND CONVERT(UPPER(j.nom || ' ' || j.cognoms), 'US7ASCII') LIKE UPPER(?) " +
                                                        "AND j.sexe LIKE ?");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psSelListJugador:\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psSelListJugador.setInt(1, e.getId());
            psSelListJugador.setString(2, "%"+nomJugador+"%");
            psSelListJugador.setString(3, sexe_busqueda);
            ResultSet rs = psSelListJugador.executeQuery();
            while (rs.next())
            {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                String cognoms = rs.getString("cognoms");
                String sexe = rs.getString("sexe");
                LocalDate data_naix = rs.getDate("data_naix").toLocalDate();
                String idlegal = rs.getString("idlegal");
                String iban = rs.getString("iban");
                LocalDate fi_revi = rs.getDate("any_fi_revi_medica").toLocalDate();
                String adresa = rs.getString("adresa");
                String foto = rs.getString("foto");
                
                Jugador j = new Jugador();
                j.setId(id);
                j.setNom(nom);
                j.setCognoms(cognoms);
                j.setSexe(sexe);
                j.setData_naix(data_naix);
                j.setIdLegal(idlegal);
                j.setIban(iban);
                j.setAny_fi_revi_medica(fi_revi);
                j.setAdresa(adresa);
                j.setFoto(foto);
                
                jugadors.add(j);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en seleccionar els jugadors de nom i/o tipus indicat:\n"+ ex.getMessage());
        }
        
        return jugadors;
    }
    
    /**
     * Retorna el jugador obtingut a partir de la seva id
     * @param idJugador id legal del jugador a obtenir
     * @return Jugador obtingut
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public Jugador obtenirJugador(String idJugador) throws GestorBDManagerException
    {
        Jugador jugador = new Jugador();
        
        if (psSelJug == null)
        {
            try
            {
                psSelJug = conn.prepareStatement("SELECT j.* FROM jugador j WHERE j.idlegal = ?");
            }
            catch(SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psSelJug", ex);
            }
        }
        
        try
        {
            psSelJug.setString(1, idJugador);
            ResultSet rs = psSelJug.executeQuery();
            while (rs.next())
            {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                String cognoms = rs.getString("cognoms");
                String sexe = rs.getString("sexe");
                LocalDate data_naix = rs.getDate("data_naix").toLocalDate();
                String idLegal = rs.getString("idLegal");
                String iban = rs.getString("iban");
                LocalDate any_fi_revi = rs.getDate("any_fi_revi_medica").toLocalDate();
                String adresa = rs.getString("adresa");
                String foto = rs.getString("foto");
                
                jugador.setId(id);
                jugador.setNom(nom);
                jugador.setCognoms(cognoms);
                jugador.setSexe(sexe);
                jugador.setData_naix(data_naix);
                jugador.setIdLegal(idLegal);
                jugador.setIban(iban);
                jugador.setAny_fi_revi_medica(any_fi_revi);
                jugador.setAdresa(adresa);
                jugador.setFoto(foto);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en seleccionar l'equip amb idLegal indicat:\n"+ ex.getMessage());
        }
        
        return jugador;
    }
    
    /**
     * Comprova si un jugador és titular a un equip.
     * 
     * @param equipId id de l'equip
     * @param jugadorId id del jugador
     * @return true si és titular, false si és convidat o no hi és
     * @throws GestorBDManagerException
    */
    @Override
    public boolean esTitular(int equipId, int jugadorId) throws GestorBDManagerException
    {
        boolean titular = false;

        if (psSelTitular == null)
        {
            try
            {
                psSelTitular = conn.prepareStatement("SELECT titular FROM membre WHERE equip = ? AND jugador = ?");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psSelTitular:\n" + ex.getMessage());
            }
        }

        ResultSet rs = null;
        try
        {
            psSelTitular.setInt(1, equipId);
            psSelTitular.setInt(2, jugadorId);
            rs = psSelTitular.executeQuery();
            if (rs.next())
            {
                String tit = rs.getString("titular");
                if (tit != null && tit.equalsIgnoreCase("Titular"))
                {
                    titular = true;
                }
            }
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error comprovant si el jugador és titular:\n" + ex.getMessage());
        }
        finally
        {
            if (rs != null)
            {
                try { rs.close(); } catch (SQLException ex) { /* Ignora */ }
            }
        }

        return titular;
    }
    
    /**
     * Insereix jugador a la BD
     * 
     * @param j Jugador a inserir
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public int afegirJugador(Jugador j) throws GestorBDManagerException
    {
        return 0;
    }
    
    /**
     * Modifica el jugador a la BD
     * 
     * @param j Jugador a modificar
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public int modificarJugador(Jugador j) throws GestorBDManagerException
    {
        return 0;
    }
    
    /**
     * ESborra el jugador de la BD
     * 
     * @param j Jugador a eliminar
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public int eliminarJugador(Jugador j) throws GestorBDManagerException
    {
        int eliminat = 0;
        
        if (psDelJug == null)
        {
            try
            {
                psDelJug = conn.prepareStatement("DELETE FROM jugador WHERE id = ?");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psDelJug:\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psDelJug.setInt(1, j.getId());
            eliminat = psDelEq.executeUpdate();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en eliminar el jugador:\n"+ ex.getMessage());
        }
        
        return eliminat;
    }
    
    /**
     * Elimina totes les relacions membre associades a un equip
     * 
     * @param equipId id de l'equip del qual es volen eliminar tots els membres
     * @return int nombre de membres eliminats
     * @throws GestorBDManagerException
    */
    @Override
    public int eliminarMembresEquip(int equipId) throws GestorBDManagerException
    {
        int eliminats = 0;

        if (psDelMembsEq == null)
        {
            try
            {
                psDelMembsEq = conn.prepareStatement("DELETE FROM membre WHERE equip = ?");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psDelMembresEquip:\n"+ ex.getMessage());
            }
        }

        try
        {
            psDelMembsEq.setInt(1, equipId);
            eliminats = psDelMembsEq.executeUpdate();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en eliminar els membres de l'equip:\n"+ ex.getMessage());
        }

        return eliminats;
    }
    
    /**
     * Obtenim les relacions de membres de la BD
     * 
     * @return Llista de membres
     * @throws GestorBDManagerException 
     */
    @Override
    public List<Membre> obtenirMembres() throws GestorBDManagerException
    {
        List<Membre> membres = new ArrayList<>();
        Statement q = null;
        try
        {
            q = conn.createStatement();
            ResultSet rs = q.executeQuery("SELECT * FROM membre");
            while (rs.next())
            {
                int equip = rs.getInt("equip");
                int jugador = rs.getInt("jugador");
                
                Membre m = new Membre();
                m.setEquMembre(equip);
                m.setJugMembre(jugador);
                switch (rs.getString("titular"))
                {
                    case "Titular":
                        m.setTitular(true);
                        break;
                        
                    case "Convidat":
                        m.setTitular(false);
                        break;                    
                }
                
                membres.add(m);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en intentar recuperar la llista de membres\n"+ ex.getMessage());
        }
        finally
        {
            if (q != null)
            {
                try
                {
                    q.close();
                }
                catch (SQLException ex)
                {
                    throw new GestorBDManagerException("Error en intentar tancar la sentència que ha recuperat la llista de membres.\n"+ ex.getMessage());
                }
            }
        }
        
        return membres;
    }
    
    /**
     * Afegeix una nova relació membre a la BD
     * 
     * @param m Membre a afegir
     * @return int Indicant la quantitat de linies afectades
     * @throws GestorBDManagerException 
     */
    @Override
    public int afegirMembre(Membre m) throws GestorBDManagerException
    {
        int inserit = 0;
        
        if (psInsMem == null)
        {
            try
            {
                psInsMem = conn.prepareStatement("INSERT INTO membre (equip, jugador, titular) VALUES (?,?,?)");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psInsMem\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psInsMem.setInt(1, m.getEquMembre());
            psInsMem.setInt(2, m.getJugMembre());
            
            if (m.getTitular())
            {
                psInsMem.setString(3, "Titular");
            }
            else 
            {
                psInsMem.setString(3, "Convidat");
            }
           
            inserit = psInsMem.executeUpdate();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en inserir el membre:\n"+ ex.getMessage());
        }
        
        
        return inserit;
    }
    
    /**
     * Modifica una relació membre a la BD
     * 
     * @param m Membre a modificar
     * @param titular Nou estat de titularitat
     * @return int Indicant la quantitat de linies afectades
     * @throws GestorBDManagerException 
     */
    @Override
    public int modificarMembre(Membre m, boolean titular) throws GestorBDManagerException
    {
        int inserit = 0;
        
        if (psModMem == null)
        {
            try
            {
                psModMem = conn.prepareStatement("UPDATE membre SET titular = ? WHERE equip = ? AND jugador = ? AND titular = ?");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psModMem:\n"+ ex.getMessage());
            }
        }
        
        try
        {
            if (titular)
            {
                psModMem.setString(1, "Titular");
            }
            else
            {
                psModMem.setString(1, "Convidat");
            }
            
            psModMem.setInt(2, m.getEquMembre());
            psModMem.setInt(3, m.getJugMembre());
            
            if (m.getTitular())
            {
                psModMem.setString(4, "Titular");
            }
            else
            {
                psModMem.setString(4, "Convidat");
            }
            inserit = psModMem.executeUpdate();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en modificar el membre:\n"+ ex.getMessage());
        }
        
        
        return inserit;
    }
    
    /**
     * Elimina una relació membre a la BD
     * 
     * @param m Membre a eliminar
     * @return int Indicant la quantitat de linies afectades
     * @throws GestorBDManagerException 
     */
    @Override
    public int eliminarMembre(Membre m) throws GestorBDManagerException
    {
        int inserit = 0;
        
        if (psDelMem == null)
        {
            try
            {
                psDelMem = conn.prepareStatement("DELETE FROM membre WHERE equip = ? AND jugador = ? AND titular = ?");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psInsMem\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psDelMem.setInt(1, m.getEquMembre());
            psDelMem.setInt(2, m.getJugMembre());
            if (m.getTitular())
            {
                psDelMem.setString(3, "Titular");
            }
            else
            {
                psDelMem.setString(3, "Convidat");
            }
            inserit = psDelMem.executeUpdate();

        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en inserir el membre:\n"+ ex.getMessage());
        }
        
        
        return inserit;
    }
    
    /**
     * Retorna llistat amb totes les categories existents
     * 
     * @return Llista de totes les categories a la BD
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public List<Categoria> obtenirCategories() throws GestorBDManagerException
    {
        List<Categoria> categories = new ArrayList<>();
        Statement q = null;
        try
        {
            q = conn.createStatement();
            ResultSet rs = q.executeQuery("SELECT * FROM categoria");
            while (rs.next())
            {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                int edat_min = rs.getInt("edat_min");
                int edat_max = rs.getInt("edat_max");
                
                if (rs.getInt("edat_max") == 0)
                {
                    edat_max = 99;
                }
                
                Categoria c = new Categoria();
                c.setId(id);
                c.setNom(nom);
                c.setEdat_min(edat_min);
                c.setEdat_max(edat_max);
                
                categories.add(c);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en intentar recuperar la llista de categories\n"+ ex.getMessage());
        }
        finally
        {
            if (q != null)
            {
                try
                {
                    q.close();
                }
                catch (SQLException ex)
                {
                    throw new GestorBDManagerException("Error en intentar tancar la sentència que ha recuperat la llista de categories.\n"+ ex.getMessage());
                }
            }
        }
        
        return categories;
    }
    
    /**
     * Retorna la categoria trobada a partir del seu nom
     * 
     * @param nom_busqueda Nom de la categoria a buscar
     * @return Categoria obtenida
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public Categoria obtenirCategoria(String nom_busqueda) throws GestorBDManagerException
    {
        Categoria cat = new Categoria();
        
        if (psSelCat == null) {
            try {
                psSelCat = conn.prepareStatement("SELECT id, nom, edat_min, edat_max FROM categoria WHERE nom = ?");
            } catch (SQLException ex) {
                throw new GestorBDManagerException("Error en preparar sentència psSelCat:\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psSelCat.setString(1, nom_busqueda);
            ResultSet rs = psSelCat.executeQuery();
            while (rs.next()) {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                Integer edat_min = rs.getInt("edat_min");
                Integer edat_max = rs.getInt("edat_max");
                cat.setId(id);
                cat.setNom(nom);
                cat.setEdat_min(edat_min);
                // Si edat max es 0 vol dir que a la BD es null, per tant asignem 99 com a edat máxima
                if (edat_max == 0)
                {
                    cat.setEdat_max(99);
                }
                else
                {
                    cat.setEdat_max(edat_max);
                }
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en obtenir categoria amb el nom indicat:\n"+ ex.getMessage());
        }
        
        return cat;
    }
    
    /**
     * Retorna l'usuari administrador de la BD loguejat amb login i contrasenya
     * 
     * @param login Identificador del administrador (mail)
     * @param pswd Contrasenya del administrador
     * @return Usuari administrador
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public int loginUsuari(String login, String pswd) throws GestorBDManagerException
    {
       int trobat = 0;
        
        if (psRetUsu == null) {
            try {
                psRetUsu = conn.prepareStatement("SELECT login, pswd FROM usuari WHERE login = ? AND pswd = ?");
            } catch (SQLException ex) {
                throw new GestorBDManagerException("Error en preparar sentència psSelCat:\n"+ ex.getMessage());
            }
        }
        
        try
        {
            psRetUsu.setString(1, login);
            psRetUsu.setString(2, pswd);
            ResultSet rs = psRetUsu.executeQuery();
            while (rs.next()) {
                trobat++;
            }
            rs.close();
        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en obtenir categoria amb el nom indicat:\n"+ ex.getMessage());
        }
        
        return trobat;
    }
    
    /**
     * Tanca la conexió amb la BD
     * 
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public void tancarCapa() throws GestorBDManagerException
    {
        if (conn != null)
        {
            try
            {
                conn.rollback();
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en fer el rollback final.\n"+ ex.getMessage());
            }
            try
            {
                conn.close();
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en tancar la connexió.\n"+ ex.getMessage());
            }
        }
    }
    
    /**
     * Conserva els canvis efectuats a la BD
     * 
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public void confirmarCanvis() throws GestorBDManagerException
    {
        try
        {
            conn.commit();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en confirmar canvis.\n"+ ex.getMessage());
        }
    }
    
    /**
     * Desfa els canvis efectuats a la BD
     * 
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException
     */
    @Override
    public void desferCanvis() throws GestorBDManagerException
    {
        try
        {
            conn.rollback();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en desfer canvis.\n"+ ex.getMessage());
        }
    }
}