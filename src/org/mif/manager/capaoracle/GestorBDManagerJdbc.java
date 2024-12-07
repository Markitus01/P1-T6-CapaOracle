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
import org.mif.manager.model.Temporada;
import org.mif.manager.model.Usuari;

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
    private PreparedStatement psSelListEquip;
    private PreparedStatement psSelEq;
    private PreparedStatement psInsEq;
    private PreparedStatement psModEq;
    private PreparedStatement psDelEq;
    private PreparedStatement psSelListJugador;
    private PreparedStatement psSelJug;
    private PreparedStatement psInsJug;
    private PreparedStatement psModJug;
    private PreparedStatement psDelJug;
    private PreparedStatement psSelCat;
    
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
            throw new GestorBDManagerException("No s'ha pogut recuperar correctament l'arxiu de configuració " +nomFitxer, ex);
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("No es pot establir la connexió.", ex);
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
            throw new GestorBDManagerException("Error en intentar recuperar la llista de temporades.\n", ex);
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
                    throw new GestorBDManagerException("Error en intentar tancar la sentència que ha recuperat la llista de temporades.\n", ex);
                }
            }
        }
        return temps;
    }
    
    /**
     * Retorna el llistat de tots els equips de la BD
     * 
     * @return Llista d'equips recuperats
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException
     */
    @Override
    public List<Equip> obtenirEquips() throws GestorBDManagerException
    {
        List<Equip> equips = new ArrayList<>();
        Statement q = null;
        try
        {
            q = conn.createStatement();
            ResultSet rs = q.executeQuery("SELECT id, nom, tipus, nom_cat, temporada FROM equip");
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
            throw new GestorBDManagerException("Error en intentar recuperar la llista d'equips\n", ex);
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
                    throw new GestorBDManagerException("Error en intentar tancar la sentència que ha recuperat la llista d'equips.\n", ex);
                }
            }
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
                psSelListEquip = conn.prepareStatement("SELECT id, nom, tipus, nom_cat, temporada FROM equip WHERE nom LIKE ? AND tipus LIKE ?");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psSelListEquip", ex);
            }
        }
        
        try
        {
            psSelListEquip.setString(1, "%"+nomEquip+"%");
            psSelListEquip.setString(2, "%"+tipusEquip+"%");
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
            throw new GestorBDManagerException("Error en seleccionar els equips de nom i/o tipus indicat", ex);
        }
        
        return equips;
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
                throw new GestorBDManagerException("Error en preparar sentència psSelEq", ex);
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
            throw new GestorBDManagerException("Error en seleccionar l'equip amb id indicat", ex);
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
                throw new GestorBDManagerException("Error en preparar sentència psInsEq", ex);
            }
        }
        
        try
        {
            psInsEq.setString(1, e.getNom());
            psInsEq.setString(2, e.getTipus());
            psInsEq.setString(3, e.getCategoria().getNom());
            psInsEq.setDate(4, Date.valueOf(e.getTemporada()));
            inserit = psInsEq.executeUpdate();

        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en inserir l'equip", ex);
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
                throw new GestorBDManagerException("Error en preparar sentència psModEq", ex);
            }
        }
        
        try
        {
            psModEq.setString(1, e.getNom());
            psModEq.setString(2, e.getTipus());
            psModEq.setString(3, e.getCategoria().getNom());
            psModEq.setDate(4, Date.valueOf(e.getTemporada()));
            modificat = psModEq.executeUpdate();

        } catch (SQLException ex) {
            throw new GestorBDManagerException("Error en modificar l'equip", ex);
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
                throw new GestorBDManagerException("Error en preparar sentència psDelEq", ex);
            }
        }
        
        try
        {
            psDelEq.setInt(1, e.getId());
            eliminat = psDelEq.executeUpdate();

        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en eliminar l'equip", ex);
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
            ResultSet rs = q.executeQuery("SELECT j.*, m.equip, m.titular FROM jugador j " +
"                                          INNER join membre m ON m.jugador = j.id " +
"                                          INNER join equip e ON e.id = m.equip");
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
                Equip eq = obtenirEquip(rs.getInt("m.equip"));
                String titular = rs.getString("m.titular");
                
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
                j.setEquip(eq);
                j.setTitular(titular);
                
                jugadors.add(j);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en intentar recuperar la llista de jugadors\n", ex);
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
                    throw new GestorBDManagerException("Error en intentar tancar la sentència que ha recuperat la llista de jugadors.\n", ex);
                }
            }
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
                psSelListJugador = conn.prepareStatement("SELECT j.*, m.equip, m.titular FROM jugador j " +
                                                        "INNER join membre m ON m.jugador = j.id " +
                                                        "INNER join equip e ON e.id = m.equip " +
                                                        "WHERE e.id = ? " +
                                                        "AND CONVERT(UPPER(j.nom || ' ' || j.cognoms), 'US7ASCII') LIKE UPPER(?) " +
                                                        "AND j.sexe LIKE ?");
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psSelListJugador", ex);
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
                int id = rs.getInt("j.id");
                String nom = rs.getString("j.nom");
                String cognoms = rs.getString("j.cognoms");
                String sexe = rs.getString("j.sexe");
                LocalDate data_naix = rs.getDate("j.data_naix").toLocalDate();
                String idlegal = rs.getString("j.idlegal");
                String iban = rs.getString("j.iban");
                LocalDate fi_revi = rs.getDate("j.any_fi_revi_medica").toLocalDate();
                String adresa = rs.getString("j.adresa");
                String foto = rs.getString("j.foto");
                Equip eq = obtenirEquip(rs.getInt("m.equip"));
                String titular = rs.getString("m.titular");
                
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
                j.setEquip(eq);
                j.setTitular(titular);
                
                jugadors.add(j);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en seleccionar els equips de nom i/o tipus indicat", ex);
        }
        
        return jugadors;
    }
    
    /**
     * Retorna el jugador obtingut a partir de la seva id
     * @param idJugador id del jugador a obtenir
     * @return Jugador obtingut
     * @throws org.mif.manager.interficiepersistencia.GestorBDManagerException 
     */
    @Override
    public Jugador obtenirJugador(int idJugador) throws GestorBDManagerException
    {
        Jugador jugador = new Jugador();
        
        if (psSelJug == null)
        {
            try
            {
                psSelJug = conn.prepareStatement("SELECT j.*, m.equip, m.titular FROM jugador "+
                                                 "INNER join membre m ON m.jugador = j.id "+
                                                 "WHERE j.id = ?");
            }
            catch(SQLException ex)
            {
                throw new GestorBDManagerException("Error en preparar sentència psSelJug", ex);
            }
        }
        
        try
        {
            psSelJug.setInt(1, idJugador);
            ResultSet rs = psSelJug.executeQuery();
            while (rs.next())
            {
                int id = rs.getInt("id");
                String nom = rs.getString("nom");
                String tipus = rs.getString("tipus");
                String nom_cat = rs.getString("nom_cat");
                LocalDate anny = rs.getDate("temporada").toLocalDate();
                jugador.setId(id);
                jugador.setNom(nom);
                jugador.setTipus(tipus);
                jugador.setCategoria(obtenirCategoria(nom_cat));
                jugador.setTemporada(anny);
            }
            rs.close();
        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en seleccionar l'equip amb id indicat", ex);
        }
        
        return equip;
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
                throw new GestorBDManagerException("Error en preparar sentència psDelJug", ex);
            }
        }
        
        try
        {
            psDelJug.setInt(1, j.getId());
            eliminat = psDelEq.executeUpdate();

        }
        catch (SQLException ex)
        {
            throw new GestorBDManagerException("Error en eliminar el jugador", ex);
        }
        
        return eliminat;
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
        return null;
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
                throw new GestorBDManagerException("Error en preparar sentència psSelCat", ex);
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
            throw new GestorBDManagerException("Error en obtenir categoria amb el nom indicat", ex);
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
    public Usuari retornarUsuari(String login, String pswd) throws GestorBDManagerException
    {
        if (conn != null)
        {
            
        }
        return null;
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
                throw new GestorBDManagerException("Error en fer el rollback final.\n", ex);
            }
            try
            {
                conn.close();
            }
            catch (SQLException ex)
            {
                throw new GestorBDManagerException("Error en tancar la connexió.\n", ex);
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
            throw new GestorBDManagerException("Error en confirmar canvis.\n", ex);
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
            throw new GestorBDManagerException("Error en desfer canvis.\n", ex);
        }
    }
}